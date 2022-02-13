#!/usr/bin/env python3
"""
antenati.py: a tool to download data from the Portale Antenati
"""

__author__      = 'Giovanni Cerretani'
__copyright__   = 'Copyright (c) 2022, Giovanni Cerretani'
__license__     = 'MIT License'
__version__     = '2.2'

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from concurrent.futures import ThreadPoolExecutor, as_completed
from json import loads
from mimetypes import guess_extension, guess_type
from os import path, mkdir, chdir
from re import search
from certifi import where
from urllib3 import PoolManager, HTTPSConnectionPool
from click import echo, confirm
from slugify import slugify
from humanize import naturalsize
from tqdm import tqdm

class AntenatiDownloader:
    """Downloader class"""

    def __init__(self, archive_url):
        self.archive_url = archive_url
        manifest = self.__get_mirador_manifest()
        self.canvases = manifest['sequences'][0]['canvases']
        self.metadata = manifest['metadata']
        self.label = manifest['label']
        self.dirname = self.__generate_dirname()
        self.gallery_length = len(self.canvases)
        self.gallery_size = 0

    def __get_mirador_manifest(self):
        """Get Mirador manifest as JSON from Portale Antenati gallery page"""
        pool_manager = PoolManager(
                        cert_reqs='CERT_REQUIRED',
                        ca_certs=where()
        )
        http_reply = pool_manager.request('GET', self.archive_url)
        html_content = http_reply.data.decode('utf-8').split('\n')
        manifest_line = next((l for l in html_content if 'manifestId' in l), None)
        if not manifest_line:
            raise RuntimeError(f'No Mirador manifest found at { self.archive_url}')
        manifest_url_pattern = search(r'\'([A-Za-z0-9.:/-]*)\'', manifest_line)
        if not manifest_url_pattern:
            raise RuntimeError(f'Invalid Mirador manifest line found at { self.archive_url}')
        manifest_url = manifest_url_pattern.group(1)
        http_reply = pool_manager.request('GET', manifest_url)
        return loads(http_reply.data.decode('utf-8'))

    def __get_metadata_content(self, label):
        """Get metadata content of Mirador manifest given its label"""
        try:
            return next((i['value'] for i in self.metadata if i['label'] == label))
        except StopIteration as exc:
            raise RuntimeError(f'Cannot get {label} from manifest') from exc

    def __generate_dirname(self):
        """Generate directory name from info in Mirador manifest"""
        archive_content_type = self.__get_metadata_content('Tipologia')
        archive_id_pattern = search(r'(\d+)', self.archive_url)
        if not archive_id_pattern:
            raise RuntimeError(f'Cannot get archive ID from {self.archive_url}')
        archive_id = archive_id_pattern.group(1)
        return slugify(f'{self.label}-{archive_content_type}-{archive_id}')

    def print_gallery_info(self):
        """Print Mirador gallery info"""
        for i in self.metadata:
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
    def __run(pool, canvas):
        url = canvas['images'][0]['resource']['@id']
        guessed_type = guess_type(url)
        guessed_extension = guess_extension(guessed_type[0])
        label = slugify(canvas['label'])
        filename = f'img_archive_{label}{guessed_extension}'
        http_reply = pool.request_encode_url('GET', url)
        with open(filename, 'wb') as img_file:
            img_file.write(http_reply.data)
        http_reply_size = len(http_reply.data)
        return http_reply_size

    def get_images(self, n_workers, n_connections):
        """Main function spanning run function in a thread pool"""
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            pool = HTTPSConnectionPool(
                            host='iiif-antenati.san.beniculturali.it',
                            maxsize=n_connections,
                            block=True,
                            cert_reqs='CERT_REQUIRED',
                            ca_certs=where()
            )
            future_img = { executor.submit(self.__run, pool, i): i for i in self.canvases }
            with tqdm(total=self.gallery_length, unit='img') as progress_results:
                for future in as_completed(future_img):
                    progress_results.update(1)
                    canvas = future_img[future]
                    label = canvas['label']
                    try:
                        size = future.result()
                    except RuntimeError as exc:
                        progress_results.write(f'{label} error ({exc})')
                    else:
                        self.gallery_size += size

    def print_summary(self):
        """Print summary"""
        print(f'Done. Total size: {naturalsize(self.gallery_size)}')

def main():
    """Main"""

    # Parse arguments
    parser = ArgumentParser(
                description=__doc__,
                epilog=__copyright__,
                formatter_class=ArgumentDefaultsHelpFormatter
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
    downloader.get_images(args.nthreads, args.nconn)

    # Print summary
    downloader.print_summary()

if __name__ == '__main__':
    main()
