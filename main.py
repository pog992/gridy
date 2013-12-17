import datetime
import jinja2
import logging
import re
import urllib
import webapp2

from google.appengine.ext import blobstore
from google.appengine.ext import db

from google.appengine.ext.webapp import blobstore_handlers

from google.appengine.api import files
from google.appengine.api import taskqueue
from google.appengine.api import users

from mapreduce import base_handler
from mapreduce import context
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import shuffler


class FileMetadata(db.Model):
    """A helper class that will hold metadata for the user's blobs.

    Specifially, we want to keep track of who uploaded it, where they uploaded it
    from (right now they can only upload from their computer, but in the future
    urlfetch would be nice to add), and links to the results of their MR jobs. To
    enable our querying to scan over our input data, we store keys in the form
    'user/date/blob_key', where 'user' is the given user's e-mail address, 'date'
    is the date and time that they uploaded the item on, and 'blob_key'
    indicates the location in the Blobstore that the item can be found at. '/'
    is not the actual separator between these values - we use '..' since it is
    an illegal set of characters for an e-mail address to contain.
    """

    __SEP = ".."
    __NEXT = "./"

    owner = db.UserProperty()
    filename = db.StringProperty()
    uploadedOn = db.DateTimeProperty()
    source = db.StringProperty()
    blobkey = db.StringProperty()
    grep_link = db.StringProperty()

    @staticmethod
    def getFirstKeyForUser(username):
        """Helper function that returns the first possible key a user could own.

        This is useful for table scanning, in conjunction with getLastKeyForUser.

        Args:
            username: The given user's e-mail address.
        Returns:
            The internal key representing the earliest possible key that a user could
            own (although the value of this key is not able to be used for actual
            user data).
        """
        return db.Key.from_path("FileMetadata", username + FileMetadata.__SEP)

    @staticmethod
    def getLastKeyForUser(username):
        """Helper function that returns the last possible key a user could own.

        This is useful for table scanning, in conjunction with getFirstKeyForUser.

        Args:
            username: The given user's e-mail address.
        Returns:
            The internal key representing the last possible key that a user could
            own (although the value of this key is not able to be used for actual
            user data).
        """
        return db.Key.from_path("FileMetadata", username + FileMetadata.__NEXT)

    @staticmethod
    def getKeyName(username, date, blob_key):
        """Returns the internal key for a particular item in the database.

        Our items are stored with keys of the form 'user/date/blob_key' ('/' is
        not the real separator, but __SEP is).

        Args:
            username: The given user's e-mail address.
            date: A datetime object representing the date and time that an input
                file was uploaded to this app.
            blob_key: The blob key corresponding to the location of the input file
                in the Blobstore.
        Returns:
            The internal key for the item specified by (username, date, blob_key).
        """
        sep = FileMetadata.__SEP
        return str(username + sep + str(date) + sep + blob_key)


class IndexHandler(webapp2.RequestHandler):
    """The main page that users will interact with, which presents users with
    the ability to upload new data or run MapReduce jobs on their existing data.
    """

    template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
                                                                        autoescape=True)

    def get(self):
        user = users.get_current_user()
        username = user.nickname()

        first = FileMetadata.getFirstKeyForUser(username)
        last = FileMetadata.getLastKeyForUser(username)

        q = FileMetadata.all()
        q.filter("__key__ >", first)
        q.filter("__key__ < ", last)
        results = q.fetch(10)

        items = [result for result in results]
        length = len(items)

        upload_url = blobstore.create_upload_url("/upload")

        self.response.out.write(self.template_env.get_template("index.html").render(
                {"username": username,
                 "items": items,
                 "length": length,
                 "upload_url": upload_url}))

    def post(self):
        filekey = self.request.get("filekey")
        blob_key = self.request.get("blobkey")
        grep_regex = self.request.get("grep")

        if grep_regex:
            pipeline = GrepPipeline(grep_regex, filekey, blob_key)
            pipeline.start()
            self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)
        else:
            return self.get()


def grep_map(data):
    ctx = context.get()
    params = ctx.mapreduce_spec.mapper.params
    regex = params['regex']
    entry, text_fn = data
    text = text_fn()
    try:
        pat = re.compile(regex)
    except:
        return
    for i, line in enumerate(text.splitlines(), 1):
        formattedLine = formatLine(pat, line)
        if formattedLine:
            yield ("<u>"+entry.filename+"</u>", '<u>%d</u>:    %s<br/><br/>' % (i, formattedLine))

def formatLine(pattern, line):
    formattedLine = ""
    prevStart = 0
    found = False
    while prevStart <= len(line):
        result = pattern.search(line,prevStart)
        if not result:
            formattedLine += line[prevStart:]
            break
        found = True
        start = result.start()
        end = result.end()
        formattedLine += line[prevStart:start]+"<b>"+line[start:end]+"</b>"
        prevStart = end;
    if found:
        return formattedLine

def grep_reduce(key, values):
    for iline in values:
        yield "%s:%s\n" % (key, iline)


class GrepPipeline(base_handler.PipelineBase):
    """A pipeline to run grep.

    Args:
        blobkey: blobkey to process as string. Should be a zip archive with
            text files inside.
    """

    def run(self, regex, filekey, blobkey):
        output = yield mapreduce_pipeline.MapreducePipeline(
                "grep",
                "main.grep_map",
                "main.grep_reduce",
                "mapreduce.input_readers.BlobstoreZipInputReader",
                "mapreduce.output_writers.BlobstoreOutputWriter",
                mapper_params={
                        "blob_key": blobkey,
                        "regex": regex,
                },
                reducer_params={
                        "mime_type": "text/plain",
                },
                shards=16)
        yield StoreOutput(filekey, output)


class StoreOutput(base_handler.PipelineBase):
    """A pipeline to store the result of the MapReduce job in the database.

    Args:
        mr_type: the type of mapreduce job run (e.g., WordCount, Index)
        encoded_key: the DB key corresponding to the metadata of this job
        output: the blobstore location where the output of the job is stored
    """

    def run(self, encoded_key, output):
        logging.debug("output is %s" % str(output))
        key = db.Key(encoded=encoded_key)
        m = FileMetadata.get(key)
        m.grep_link = output[0].replace("blobstore","blobstore_format")
        m.put()


class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
    """Handler to upload data to blobstore."""

    def post(self):
        source = "uploaded by user"
        upload_files = self.get_uploads("file")
        blob_key = upload_files[0].key()
        name = self.request.get("name")

        user = users.get_current_user()

        username = user.nickname()
        date = datetime.datetime.now()
        str_blob_key = str(blob_key)
        key = FileMetadata.getKeyName(username, date, str_blob_key)

        m = FileMetadata(key_name = key)
        m.owner = user
        m.filename = name
        m.uploadedOn = date
        m.source = source
        m.blobkey = str_blob_key
        m.put()

        self.redirect("/")


class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
    """Handler to download blob by blobkey."""

    def get(self, key):
        key = str(urllib.unquote(key)).strip()
        logging.debug("key is %s" % key)
        blob_info = blobstore.BlobInfo.get(key)
        self.send_blob(blob_info)

class FormatterHandler(blobstore_handlers.BlobstoreDownloadHandler):
    """Handler to download blob by blobkey."""

    def get(self, key):
        key = str(urllib.unquote(key)).strip()
        logging.debug("key is %s" % key)
        blob_info = blobstore.BlobInfo.get(key)
        self.send_blob(blob_info, "text/html")

app = webapp2.WSGIApplication(
        [
                ('/', IndexHandler),
                ('/upload', UploadHandler),
                (r'/blobstore/(.*)', DownloadHandler),
                (r'/blobstore_format/(.*)', FormatterHandler),
        ],
        debug=True)
