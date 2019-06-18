import asyncio
import requests

import subprocess

from concurrent.futures import ThreadPoolExecutor


import os
import shutil

from seleniumwire import webdriver

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec


class EpisodeDownloader:
    """ Open target page, scroll to player window and launch play,
    initiating ajax to get playlist urls.
    Skip adds and get all chunks of the video stream, load them,
    save and concatenate to a single file
    """

    def __init__(self, driver, start_url):
        """ obj instance created in context of driver process """

        self.driver = driver
        self.start_url = start_url
        self.stream_link = self._get_stream_link()
        self.stream_list = self._get_stream_list()
        self.folder = start_url[-8:].replace(':', '')
        os.mkdir(self.folder)

    def _get_stream_link(self):
        """ cannot switch to player frame until it's in a viewport
        index.m3u8 - video stream playlist
        returns string of ajax url of playlist
        """

        self.driver.get(self.start_url)

        player = self.driver.find_element_by_id('player')
        self.driver.execute_script("arguments[0].scrollIntoView();", player)

        self.driver.switch_to.frame('cdn-player')
        elem = WebDriverWait(self.driver, 10).until(
           ec.presence_of_element_located((By.ID, 'play_button'))
        )
        elem.click()
        self.driver.wait_for_request('/index.m3u8')
        # seleniumwire .request tracks all requests done by browser
        for request in self.driver.requests:
            if request.response and 'index.m3u8' in request.path:
                stream_link = request.response.body.decode('utf-8').split('\n')[2]
                return stream_link

    def _get_stream_list(self):
        """ get all file names from .m3u8 video stream playlist """

        file = requests.get(self.stream_link).content
        stream_list = file.decode('utf-8').split('\n')
        stream_list.remove('')
        return stream_list[7::2]

    def _get_stream_file(self, file_name):
        """ compose ajax for chunk of video stream from playlist  """

        file_link = os.path.split(self.stream_link)[0]
        return os.path.join(file_link, file_name)

    @staticmethod
    def _load_file(stream_file):
        """ return tuple of video stream chunk file name as string
        and chunk itself as binary
        """

        response, cont_len, status = None, None, None
        file_len = 1
        while status != 200 and cont_len != file_len:
            response = requests.get(stream_file)
            status = response.status_code
            cont_len = response.headers['content-length']
            file_len = len(response.content)
            print(":   ".join((os.path.split(stream_file)[1],
                              status,
                              cont_len,
                              file_len,))
                  )
        return stream_file, response.content

    def _write_content(self, file_content: tuple):
        """ save binary of video stream chunk in to a file"""

        file_name = os.path.split(file_content[0])[1]
        file_path = './'+'/'.join((self.folder, file_name,))
        try:
            with open(file_path, 'wb') as segment:
                segment.write(file_content[1])
        except FileNotFoundError:
            print('FileNotFoundError: ', file_path)

    async def __load_all_files(self):
        """ asynchronous get all chunks from stream playlist with ajax requests
        and save to the disc
        """

        with ThreadPoolExecutor(max_workers=25) as requester:
            loop = asyncio.get_event_loop()
            task = [
                loop.run_in_executor(requester, self._load_file, file_url)
                for file_url in list(map(self._get_stream_file, self.stream_list))
            ]
            for response_content in await asyncio.gather(*task):
                self._write_content(response_content)

    def get_all_stream(self):
        """ await handler """

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.__load_all_files())
        loop.run_until_complete(future)

    def make_single_file(self):
        """ glue all chunks of the stream into a single video file


        :return:
        """

        stream_list = list(map(
            (lambda x: '/'.join(('.', self.folder, x,))),
            self.stream_list)
        )[:]
        command_string = 'concat:' + '|'.join(stream_list)
        subprocess.call(['ffmpeg', '-i', command_string, '-c', 'copy', './'+self.folder+'.ts'])
        for file in stream_list:
            os.remove(file)
        shutil.move('./'+self.folder+'.ts', './'+self.folder+'/'+self.folder+'.ts')


class SeasonDownloader:
    pass


if __name__ == "__main__":

    start_url = "https://rezka.ag/cartoons/comedy/2136-rik-i-morti-2013.html#t:66-s:1-e:11"

    with webdriver.Firefox() as driver:
        episode11 = EpisodeDownloader(driver, start_url)
        episode11.get_all_stream()
        episode11.make_single_file()
