#!/usr/bin/env python3
"""
antenati.py: a tool to download data from the Portale Antenati
"""

__author__      = 'Giovanni Cerretani'
__copyright__   = 'Copyright (c) 2022, Giovanni Cerretani'
__license__     = 'MIT License'
__version__     = '2.3'

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from concurrent.futures import ThreadPoolExecutor, as_completed
from cgi import parse_header
from json import loads
from mimetypes import guess_extension
from os import path, mkdir, chdir
from random import randint
from re import search
from certifi import where
from urllib3 import PoolManager, HTTPSConnectionPool, HTTPResponse, make_headers
from click import echo, confirm
from slugify import slugify
from humanize import naturalsize
from tqdm import tqdm


class AntenatiDownloader:
    """Downloader for Portale Antenati"""

    def __init__(self, archive_url):
        self.archive_url = archive_url
        self.archive_id = self.__get_archive_id(self.archive_url)
        self.manifest = self.__get_iiif_manifest(self.archive_url)
        self.canvases = self.manifest['sequences'][0]['canvases']
        self.dirname = self.__generate_dirname()
        self.gallery_length = len(self.canvases)
        self.gallery_size = 0
        self.active_executors = []
        self.active_progress_bars = []

    @staticmethod
    def __http_headers():
        """Generate HTTP headers to improve speed and to behave as a browser"""
        # Default headers to reduce data transfers
        headers = make_headers(
            keep_alive=True,
            accept_encoding=True
        )
        # Update 05/2022:
        # SAN server return 403 if HTTP headers are not properly set.
        # - User-Agent: not required, but was required in the past
        # - Referer: required
        # - Origin: not required
        # Not required headers are kept, in case new filters are added.
        ver = f'{randint(80, 97)}.0'
        headers['User-Agent'] = f'Mozilla/5.0 (Mobile; rv:{ver}) Gecko/{ver} Firefox/{ver}'
        headers['Referer'] = 'https://www.antenati.san.beniculturali.it/'
        headers['Origin'] = 'https://www.antenati.san.beniculturali.it'
        return headers

    @staticmethod
    def __get_archive_id(url):
        """Get numeric archive ID from the URL"""
        archive_id_pattern = search(r'(\d+)', url)
        if not archive_id_pattern:
            raise RuntimeError(f'Cannot get archive ID from {url}')
        return archive_id_pattern.group(1)

    @staticmethod
    def __get_iiif_manifest(url):
        """Get IIIF manifest as JSON from Portale Antenati gallery page"""
        pool = PoolManager(
            headers=AntenatiDownloader.__http_headers(),
            cert_reqs='CERT_REQUIRED',
            ca_certs=where()
        )
        http_reply = pool.request('GET', url)
        assert isinstance(http_reply, HTTPResponse)
        if http_reply.status != 200:
            raise RuntimeError(f'{url}: HTTP error {http_reply.status}')
        content_type = parse_header(http_reply.headers['Content-Type'])
        html_content = http_reply.data.decode(content_type[1]['charset']).split('\n')
        manifest_line = next((l for l in html_content if 'manifestId' in l), None)
        if not manifest_line:
            raise RuntimeError(f'No IIIF manifest found at {url}')
        manifest_url_pattern = search(r'\'([A-Za-z0-9.:/-]*)\'', manifest_line)
        if not manifest_url_pattern:
            raise RuntimeError(f'Invalid IIIF manifest line found at {url}')
        manifest_url = manifest_url_pattern.group(1)
        http_reply = pool.request('GET', manifest_url)
        assert isinstance(http_reply, HTTPResponse)
        if http_reply.status != 200:
            raise RuntimeError(f'{url}: HTTP error {http_reply.status}')
        content_type = parse_header(http_reply.headers['Content-Type'])
        return loads(http_reply.data.decode(content_type[1]['charset']))

    def __get_metadata_content(self, label):
        """Get metadata content of IIIF manifest given its label"""
        try:
            return next((i['value'] for i in self.manifest['metadata'] if i['label'] == label))
        except StopIteration as exc:
            raise RuntimeError(f'Cannot get {label} from manifest') from exc

    def __generate_dirname(self):
        """Generate directory name from info in IIIF manifest"""
        archive_context = self.__get_metadata_content('Contesto archivistico')
        archive_year = self.__get_metadata_content('Titolo')
        archive_typology = self.__get_metadata_content('Tipologia')
        return slugify(f'{archive_context}-{archive_year}-{archive_typology}-{self.archive_id}')

    def print_gallery_info(self):
        """Print IIIF gallery info"""
        for i in self.manifest['metadata']:
            label = i['label']
            value = i['value']
            print(f'{label:<25}{value}')
        print(f'{self.gallery_length} images found.')

    def check_dir(self):
        """Check if directory already exists and chdir to it"""
        print(f'Output directory: {self.dirname}')
        if path.exists(self.dirname):
            echo(f'Directory {self.dirname} already exists.')
            confirm('Do you want to proceed?', abort=True)
        else:
            mkdir(self.dirname)
        chdir(self.dirname)

    @staticmethod
    def __thread_main(pool, canvas):
        assert isinstance(pool, HTTPSConnectionPool)
        url = canvas['images'][0]['resource']['@id']
        http_reply = pool.request('GET', url)
        assert isinstance(http_reply, HTTPResponse)
        if http_reply.status != 200:
            raise RuntimeError(f'{url}: HTTP error {http_reply.status}')
        content_type = parse_header(http_reply.headers['Content-Type'])
        extension = guess_extension(content_type[0])
        if not extension:
            raise RuntimeError(f'{url}: Unable to guess extension "{content_type[0]}"')
        label = slugify(canvas['label'])
        filename = f'{label}{extension}'
        with open(filename, 'wb') as img_file:
            img_file.write(http_reply.data)
        http_reply_size = len(http_reply.data)
        return http_reply_size

    @staticmethod
    def __executor(max_workers):
        return ThreadPoolExecutor(max_workers=max_workers)

    @staticmethod
    def __pool(maxsize):
        return HTTPSConnectionPool(
            host='iiif-antenati.san.beniculturali.it',
            maxsize=maxsize,
            block=True,
            headers=AntenatiDownloader.__http_headers(),
            cert_reqs='CERT_REQUIRED',
            ca_certs=where()
        )

    @staticmethod
    def __progress(total):
        return tqdm(total=total, unit='img')

    def run(self, n_workers, n_connections):
        """Download images using a thread pool"""
        with self.__executor(n_workers) as executor, self.__pool(n_connections) as pool:
            self.active_executors.append(executor)
            future_img = { executor.submit(self.__thread_main, pool, i): i for i in self.canvases }
            with self.__progress(self.gallery_length) as progress:
                self.active_progress_bars.append(progress)
                for future in as_completed(future_img):
                    if future.cancelled():
                        continue
                    progress.update()
                    canvas = future_img[future]
                    label = canvas['label']
                    try:
                        size = future.result()
                    except RuntimeError as exc:
                        progress.write(f'{label} error ({exc})')
                    else:
                        self.gallery_size += size
                self.active_progress_bars.remove(progress)
            self.active_executors.remove(executor)

    def cancel(self):
        for progress_bar in self.active_progress_bars:
            progress_bar.disable = True
        for executor in self.active_executors:
            executor.shutdown(cancel_futures=True)

    def print_summary(self):
        """Print summary"""
        print(f'Done. Total size: {naturalsize(self.gallery_size)}')


def main():
    from signal import signal, SIGINT

    def signal_handler(signum, frame):
        print(f'Canceling...')
        if downloader is not None:
            downloader.cancel()
        exit(1)

    downloader = None
    signal(SIGINT, signal_handler)

    # Parse arguments
    parser = ArgumentParser(
        description=__doc__,
        epilog=__copyright__,
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('url', metavar='URL', type=str, help='url of the gallery page')
    parser.add_argument('-n', '--nthreads', type=int, help='max n. of threads', default=8)
    parser.add_argument('-c', '--nconn', type=int, help='max n. of connections', default=4)
    parser.add_argument('-v', '--version', action='version', version=__version__)
    args = parser.parse_args()

    # Initialize
    downloader = AntenatiDownloader(args.url)

    # Print gallery info
    downloader.print_gallery_info()

    # Check if directory already exists and chdir to it
    downloader.check_dir()

    # Run
    downloader.run(args.nthreads, args.nconn)

    # Print summary
    downloader.print_summary()


if __name__ == '__main__':
    main()
