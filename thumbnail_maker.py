# thumbnail_maker.py
import time
import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve
from threading import Thread
from queue import Queue


import PIL
from PIL import Image

FORMAT = "[%(threadName)s, %(asctime)s, %(levelname)s, %(message)s]"
logging.basicConfig(filename='logfile.log', level=logging.DEBUG, format = FORMAT)

class ThumbnailMakerService(object):
    '''
    Download and resizing methods are separated to maintain concurrency best practices.
    Downloading is I/O intensive, while reshaping is CPU intensive
    '''
    def __init__(self, home_dir='.'):
        self.home_dir = home_dir
        self.input_dir = self.home_dir + os.path.sep + 'incoming' # Directory where image files are loaded from
        self.output_dir = self.home_dir + os.path.sep + 'outgoing' # Directory where resized images are placed
        self.img_queue = Queue()
        self.dl_queue = Queue()

    def download_image(self):
        while not self.dl_queue.empty():
            '''
            Avoids situation where 1 object left in queue and one thread
            reads a valid value while another thread acceses and dequeues the object,
            but the other thread proceeds to read
            '''
            try:
                url = self.dl_queue.get(block = False)
                # download each image and save to the input dir 
                img_filename = urlparse(url).path.split('/')[-1]
                urlretrieve(url, self.input_dir + os.path.sep + img_filename)
                self.img_queue.put(img_filename)

                self.dl_queue.task_done()
            except Queue.Empty:
                logging.info('Queue Empty')

    def download_images(self, img_url_list):
        # validate inputs
        logging.info("beginning image downloads")

        start = time.perf_counter()
        for url in img_url_list:
            # download each image and save to the input dir 
            img_filename = urlparse(url).path.split('/')[-1]
            urlretrieve(url, self.input_dir + os.path.sep + img_filename)
            self.img_queue.put(img_filename)
        end = time.perf_counter()
        self.img_queue.put(None)
        logging.info("downloaded {} images in {} seconds".format(len(img_url_list), end - start))
	

    def perform_resizing(self):
        os.makedirs(self.output_dir, exist_ok=True)

        logging.info("beginning image resizing")
        target_sizes = [32, 64, 200]
        num_images = len(os.listdir(self.input_dir))

        start = time.perf_counter()
        while True:
            filename = self.img_queue.get()
            if filename:
                logging.info("Resizing image {}".format(filename))
                orig_img = Image.open(self.input_dir + os.path.sep + filename)
                for basewidth in target_sizes:
                    img = orig_img
                    # calculate target height of the resized image to maintain the aspect ratio
                    wpercent = (basewidth / float(img.size[0]))
                    hsize = int((float(img.size[1]) * float(wpercent)))
                    # perform resizing
                    img = img.resize((basewidth, hsize), PIL.Image.LANCZOS)
                    
                    # save the resized image to the output dir with a modified file name 
                    new_filename = os.path.splitext(filename)[0] + \
                        '_' + str(basewidth) + os.path.splitext(filename)[1]
                    img.save(self.output_dir + os.path.sep + new_filename)

                os.remove(self.input_dir + os.path.sep + filename)
                logging.info("Done resizing image {}".format(filename))
                self.img_queue.task_done()
            else:
                self.img_queue.task_done()
                break
                
        end = time.perf_counter()
        logging.info("created {} thumbnails in {} seconds".format(num_images, end - start))

    def make_thumbnails(self, img_url_list):
        logging.info("START make_thumbnails")
        start = time.perf_counter()

        map( # Mapping the put method of the queue object to the url list
            lambda img_url: self.dl_queue.put(img_url), 
            img_url_list
            )

        num_dl_threads = 4

        for _ in range(num_dl_threads):
            thread1 = Thread(target = self.download_images)
            thread1.start()
            # Join does not need to be called because a join blocks 
            # the calling thread until the join thread is complete
            # The threads are more dependent upon the resizing action
            # as opposed to the download action
        thread2 = Thread(target = self.perform_resizing)
        thread2.start()

        self.dl_queue.join()
        self.img_queue.put(None)

        thread2.join()

        end = time.perf_counter()
        logging.info("END make_thumbnails in {} seconds".format(end - start))
    