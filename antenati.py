#!/usr/bin/env python3
"""
antenati.py: a tool for downloading images from Portale Antenati
"""

__author__      = 'Giovanni Cerretani'
__copyright__   = 'Copyright (c) 2022, Giovanni Cerretani'
__license__     = 'MIT License'
__version__     = '2.3'

import cgi
import json
import mimetypes
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import chdir, cpu_count, mkdir, path

import certifi
import click
import cloup
from humanize import naturalsize
from slugify import slugify
from tqdm import tqdm
from urllib3 import HTTPResponse, HTTPSConnectionPool, PoolManager, make_headers


context_settings = cloup.Context.settings(
    help_option_names=['--help', '-h'],
    show_default=True,
)


class PageListParamType(click.ParamType):
    name = "page list"

    def convert(self, value, param, ctx):
        if isinstance(value, list):
            return value

        def parse_int(value):
            try:
                return int(value)
            except ValueError:
                self.fail(f'{value!r} is not a valid integer', param, ctx)

        pages = []

        for part in value.split(","):
            index_range = [parse_int(p) for p in part.split("-", 1)]
            if len(index_range) == 2:
                pages.extend(range(index_range[0], index_range[1] + 1))
            else:
                pages.append(index_range[0])

        return pages


PAGE_LIST = PageListParamType()


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
        """Generate HTTP headers to improve speed and to behave like a browser"""
        # Default headers to reduce data transfers
        headers = make_headers(
            keep_alive=True,
            accept_encoding=True
        )
        # SAN server return 403 if HTTP headers are not properly set.
        # - User-Agent: not required, but was required in the past
        # - Referer: required
        # - Origin: not required
        # Not required headers are kept, in case new filters are added.
        headers['User-Agent'] = 'Mozilla/5.0 (Mobile; rv:97.0) Gecko/97.0 Firefox/97.0'
        headers['Referer'] = 'https://antenati.cultura.gov.it/'
        headers['Origin'] = 'https://antenati.cultura.gov.it'
        return headers

    @staticmethod
    def __get_archive_id(url):
        """Get numeric archive ID from URL"""
        archive_id_pattern = re.search(r'(\d+)', url)
        if not archive_id_pattern:
            raise RuntimeError(f'Cannot get archive ID from {url}')
        return archive_id_pattern.group(1)

    @staticmethod
    def __get_iiif_manifest(url):
        """Get IIIF manifest as JSON from gallery web page URL"""
        pool = PoolManager(
            headers=AntenatiDownloader.__http_headers(),
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )
        http_reply = pool.request('GET', url)
        assert isinstance(http_reply, HTTPResponse)
        if http_reply.status != 200:
            raise RuntimeError(f'{url}: HTTP error {http_reply.status}')
        content_type = cgi.parse_header(http_reply.headers['Content-Type'])
        html_content = http_reply.data.decode(content_type[1]['charset']).split('\n')
        manifest_line = next((l for l in html_content if 'manifestId' in l), None)
        if not manifest_line:
            raise RuntimeError(f'No IIIF manifest found at {url}')
        manifest_url_pattern = re.search(r'\'([A-Za-z0-9.:/-]*)\'', manifest_line)
        if not manifest_url_pattern:
            raise RuntimeError(f'Invalid IIIF manifest line found at {url}')
        manifest_url = manifest_url_pattern.group(1)
        http_reply = pool.request('GET', manifest_url)
        assert isinstance(http_reply, HTTPResponse)
        if http_reply.status != 200:
            raise RuntimeError(f'{url}: HTTP error {http_reply.status}')
        content_type = cgi.parse_header(http_reply.headers['Content-Type'])
        return json.loads(http_reply.data.decode(content_type[1]['charset']))

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
            click.echo(f'{label:<25}{value}')
        click.echo(f'{self.gallery_length} images found.')

    def check_dir(self, force=False):
        """Check if directory already exists and chdir to it"""
        click.echo(f'Output directory: {self.dirname}')
        if path.exists(self.dirname):
            if not force:
                click.echo(f'Directory {self.dirname} already exists.')
                click.confirm('Do you want to proceed?', abort=True)
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
        content_type = cgi.parse_header(http_reply.headers['Content-Type'])
        extension = mimetypes.guess_extension(content_type[0])
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
            host='iiif-antenati.cultura.gov.it',
            maxsize=maxsize,
            block=True,
            headers=AntenatiDownloader.__http_headers(),
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )

    @staticmethod
    def __progress(total):
        return tqdm(total=total, unit='img')

    def run(self, pages, n_workers, n_connections):
        """Download images using a thread pool"""
        with self.__executor(n_workers) as executor, self.__pool(n_connections) as pool:
            self.active_executors.append(executor)
            if pages is not None:
                canvases = [self.canvases[p - 1] for p in pages]
            else:
                canvases = self.canvases
            future_img = { executor.submit(self.__thread_main, pool, i): i for i in canvases }
            with self.__progress(len(canvases)) as progress:
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
        click.echo(f'Done. Total size: {naturalsize(self.gallery_size)}')


def parse_pages(pages):
    pass


@cloup.command(help=__doc__, epilog=__copyright__, context_settings=context_settings)
@cloup.option('--force', '-f', is_flag=True, help='Do not prompt if download directory already exists.')
@cloup.option('--nthreads', '-n', type=int, default=cpu_count(), help='Maximum number of threads to use.')
@cloup.option('--nconns', '-c', type=int, default=4, help='Maximum number of connections to use.')
@cloup.version_option(__version__, '--version', '-v', message="%(prog)s v%(version)s")
@cloup.argument('url', type=str, help='URL of the gallery web page.')
@cloup.argument('pages', type=PAGE_LIST, required=False, help='Gallery pages to download.')
def main(force, nthreads, nconns, url, pages):
    from signal import signal, SIGINT

    downloader = None

    def signal_handler(signum, frame):
        click.echo()
        click.echo(f'Canceling...')
        if downloader is not None:
            downloader.cancel()
        exit(1)

    signal(SIGINT, signal_handler)

    try:
        downloader = AntenatiDownloader(url)

        downloader.print_gallery_info()

        if pages is not None:
            # Check that gallery page numbers are in range.
            for p in pages:
                if p < 0 or p >= len(downloader.canvases):
                    raise RuntimeError(f'Gallery age {p} is out of range')

            pages = set(pages)

        downloader.check_dir(force)
        downloader.run(pages, nthreads, nconns)
        downloader.print_summary()
    except RuntimeError as exc:
        click.echo(f'Error: {exc}', file=sys.stderr)


if __name__ == '__main__':
    main()
