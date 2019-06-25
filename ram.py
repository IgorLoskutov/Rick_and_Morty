import asyncio
import requests

from bs4 import BeautifulSoup

import subprocess

from concurrent.futures import ThreadPoolExecutor

from time import time

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

    def __init__(self, start_url):
        """ obj instance created in context of driver process """
        self.driver = webdriver.Firefox()
        self.start_url = start_url
        self.streams_links = self._get_episodes()
        self.streams_list = None  # delivered by   __get_chunks()
        self._get_chunks()
        # self.folder = start_url[-8:].replace(':', '').replace('-', '')
        # if not os.path.exists(self.folder):
        #     os.mkdir(self.folder)

    def _get_episodes(self):
        playlists_links = dict()
        self.driver.get(self.start_url)

        # scroll page to see iframe in viewport
        player = self.driver.find_element_by_id('player')
        self.driver.execute_script("arguments[0].scrollIntoView();", player)

        # click play_button for each episode to catch link to its playlist
        episodes_buttons = self.driver.find_elements_by_xpath(
            "//ul[@id='simple-episodes-list-1']/li[@data-season_id='1']"
        )
        for episode in episodes_buttons:
            episode.click()
            WebDriverWait(self.driver, 30).until(
                ec.frame_to_be_available_and_switch_to_it('cdn-player'))
            elem = WebDriverWait(self.driver, 30).until(
                ec.element_to_be_clickable((By.ID, 'play_button'))
            )
            elem.click()
            self.driver.switch_to.default_content()
            self.driver.wait_for_request('/index.m3u8', timeout=60)

            # seleniumwire .request tracks all requests done by browser
            for request in self.driver.requests:
                if request.response and "/index.m3u8" in request.path:
                    playlists_links[episode.text] = \
                        request.response.body.decode('utf-8').split('\n')[4]

            self.driver.switch_to.default_content()
            del self.driver.requests  # clean the list of requests tracked by seleniumwire

        self.driver.close()
        return playlists_links

    def _get_chunks(self):
        """ get all file names from .m3u8 video streams playlists """

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.__load_stream_files())
        loop.run_until_complete(future)

    async def __load_stream_files(self):
        """ asynchronous get of all .m3u8  playlists and attach list of chunk names
        to the episode name as key in the dict
        """
        self.streams_list = dict()
        with ThreadPoolExecutor(max_workers=25) as requester:
            loop = asyncio.get_event_loop()

            task = [
                loop.run_in_executor(requester, self._load_file, episode, file_url)
                for episode, file_url in self.streams_links.items()
            ]
            for file_loaded in await asyncio.gather(*task):
                self.streams_list[file_loaded[0]] = self._chunks_list(file_loaded[1])

    @staticmethod
    def _load_file(file_name, file_url):
        """ return: tuple of file name and binary file itself downloaded """

        return file_name, requests.get(file_url).content

    @staticmethod
    def _chunks_list(file_data):
        """read playlist and pick file names mentioned in it"""

        lst = file_data.decode('utf-8').split('\n')
        lst.remove('')
        return lst[7::2]

    @staticmethod
    def _chunk_url(plailist_url, chunks_list):
        """ return the list of chunk_urls for files from playlist """

        location = plailist_url.split('/')[:-1]
        location_url = '/'.join(location)

        chunks_url_list = list(
            map(
                lambda x: '/'.join((location_url, x,)),
                chunks_list
            )
        )
        return chunks_url_list

    @staticmethod
    def req_get(url):
        """ request get + print(url) to display that requests goes async-ly
         just for visualization than program not hanged
         """

        rqst = requests.get(url)
        print(rqst.url)
        return rqst

    async def load_episode(self, episode: tuple):
        """loads all chunks of episode
        episode input is dict.items() : (episode, list of chunk_urls)
        save them into a folder named after episode
        """

        with ThreadPoolExecutor(max_workers=25) as requester:
            loop = asyncio.get_event_loop()

            task = [
                loop.run_in_executor(requester, self.req_get, url)
                for url in episode[1]
            ]
            for chunk_loaded in await asyncio.gather(*task):

                chunk_name = chunk_loaded.url.split('/')[-1]
                print(f"./{episode[0]}/{chunk_name}")
                if not os.path.exists(f"./{episode[0]}"):
                    os.mkdir(f"./{episode[0]}")
                with open(f"./{episode[0]}/{chunk_name}", 'wb') as chunk:
                    chunk.write(chunk_loaded.content)

    def download_episodes(self):
        """ download all episodes one by one """

        for episode in self.streams_links:
            ep_tuple = (
                episode,
                self._chunk_url(self.streams_links[episode],
                                self.streams_list[episode]
                                )
            )

            loop = asyncio.get_event_loop()
            future = asyncio.ensure_future(self.load_episode(ep_tuple))
            loop.run_until_complete(future)

    def make_single_file(self):
        """ glue all chunks of the stream into a single video file """

        for episode in self.streams_list:
            chunk_list = list(map(
                (lambda x: '/'.join(('.', episode, x,))),
                self.streams_list[episode])
            )[:]
            command_string = 'concat:' + '|'.join(chunk_list)
            subprocess.call(['ffmpeg',
                             '-i',
                             command_string,
                             '-c',
                             'copy',
                             './' + episode + '.ts'])

        for folder in self.streams_list:
            shutil.rmtree('./'+folder, ignore_errors=False, onerror=None)


class SeasonDownloader:
    pass


if __name__ == "__main__":
    start = time()
    season_1_url = "https://rezka.ag/cartoons/comedy/2136-rik-i-morti-2013.html#t:66-s:1-e:1"

    season_1 = EpisodeDownloader(season_1_url)

    season_1.download_episodes()
    season_1.make_single_file()

    end = time()
    print(end - start)
