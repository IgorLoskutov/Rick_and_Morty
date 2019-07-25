import asyncio
import requests

import subprocess

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor

from time import time, strftime, localtime

import os
import sys
import getopt
import shutil

from seleniumwire import webdriver

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec


class EpisodeDownloader:
    """ Open target page, scroll to player window and launch play,
    initiating ajax to get playlist urls.
    Skip adds and get all chunks of the video stream, load them,
    save and concatenate to a single file
    """

    def __init__(self, start_url, season):
        """ obj instance created in context of driver process """
        profile = webdriver.FirefoxProfile()
        profile.set_preference("permissions.default.image", 2)
        # 1 - Allow all images
        # 2 - Block all images
        # 3 - Block 3rd party images

        self.driver = webdriver.Firefox(firefox_profile=profile)
        self.season = season
        self.start_url = start_url
        self.streams_links = self._get_episodes()
        self.streams_list = None  # delivered by _get_chunks()
        self._get_chunks()

    def _get_episodes(self):

        playlists_links = dict()
        attempt = 1
        while attempt <= 4:
            try:
                self.driver.get(self.start_url)
                print(f"getting season{self.season} episodes started attempt {attempt}")
                # scroll page to see frame in viewport
                player = self.driver.find_element_by_id('player')
                self.driver.execute_script("arguments[0].scrollIntoView();", player)

                # click play_button for each episode to catch link to its playlist

                episodes_buttons = self.driver.find_elements_by_xpath(
                    f"//ul[@id='simple-episodes-list-{self.season}']/li[@data-season_id]"
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
                    self.driver.wait_for_request('/index.m3u8', timeout=10)

                    # seleniumwire .request tracks all requests done by browser
                    for request in self.driver.requests:
                        if request.response and "/index.m3u8" in request.path:
                            playlists_links[episode.text] = \
                                request.response.body.decode('utf-8').split('\n')[4]

                self.driver.switch_to.default_content()
                del self.driver.requests  # clean the list of requests tracked by seleniumwire
                print(f"getting season{self.season} episodes complete")
                break
            except TimeoutException:
                if attempt > 4:
                    print(f"loading season {self.season} episodes list failed after 4 attempts ")
                    break
                attempt += 1
                self.driver.switch_to.default_content()
                del self.driver.requests  # clean the list of requests tracked by seleniumwire
                continue
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
    def _chunk_url(playlist_url, chunks_list):
        """ return the list of chunk_urls for files from playlist """

        location = playlist_url.split('/')[:-1]
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
         just for visualization that program not hanged
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
        """ glue all chunks of the stream into a single video file
        ffmpeg required for processing vide ofiles"""

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
    """Download all seasons
    Check number of seasons - Open target page, scroll to player window and find seasons list.

    """
    def __init__(self, start_url):
        profile = webdriver.FirefoxProfile()
        profile.set_preference("permissions.default.image", 2)
        # 1 - Allow all images
        # 2 - Block all images
        # 3 - Block 3rd party images
        self.driver = webdriver.Firefox(firefox_profile=profile)
        self.start_url = start_url
        self.seasons_urls = self._get_seasons()

    def _get_seasons(self):
        self.driver.get(self.start_url)
        player = self.driver.find_element_by_id('player')
        self.driver.execute_script("arguments[0].scrollIntoView();", player)
        seasons_buttons = self.driver.find_elements_by_xpath(
            "//ul[@id='simple-seasons-tabs']/li[@data-tab_id]"
        )

        seasons = [_.get_attribute('data-tab_id') for
                   _ in seasons_buttons]
        s_url = self.start_url + '#t:{}-s:{}-e:1'

        seasons_urls = {se: s_url.format(self._get_trans(), se) for se in seasons}

        self.driver.close()
        return seasons_urls

    def _get_trans(self):
        return self.driver.find_elements_by_xpath(
            "//ul[@id='translators-list']/li[@*]"
        )[0].get_attribute('data-translator_id')


if __name__ == "__main__":
    start = time()
    season_1_url = ''
    try:
        if not sys.argv[1:]:
            raise getopt.GetoptError('missig url')
        opts, args = getopt.getopt(sys.argv[1:], "hu:", ["url="])
    except getopt.GetoptError:
        print('ram.py -u <url>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('ram.py -u <url>')
            sys.exit()
        elif opt in ("-u", "--url"):
            season_1_url = arg

    mov = SeasonDownloader(season_1_url)
    print(mov.seasons_urls)

    def multi(url_season: tuple):
        season_folder = f"season{url_season[0]} - {strftime('%H:%M:%S', localtime())}"
        os.mkdir(season_folder)
        os.chdir(season_folder)
        season = EpisodeDownloader(url_season[1], url_season[0])

        season.download_episodes()
        try:
            season.make_single_file()
        except FileNotFoundError:
            print("This program require ffmpeg installed. Please install and try again")
            return
        finally:
            os.chdir("../")

    with ProcessPoolExecutor() as executor:
        executor.map(multi, mov.seasons_urls.items())

    end = time()
    print(end - start)
    

