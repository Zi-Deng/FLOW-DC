"""Download a tiny loopback fixture using only the core installation."""
import argparse
import struct
import threading
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from flowdc.cli import main as flowdc


def png():
    def chunk(kind,data):
        return struct.pack('!I',len(data))+kind+data+struct.pack('!I',zlib.crc32(kind+data))
    return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('!IIBBBBB',1,1,8,6,0,0,0))+chunk(b'IDAT',zlib.compress(b'\0\x20\x60\xa0\xff'))+chunk(b'IEND',b'')


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body=png();self.send_response(200);self.send_header('Content-Type','image/png');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def log_message(self,*args):pass


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output-dir',type=Path,default=Path('build/demo-download'));args=parser.parse_args()
    if args.output_dir.exists():parser.error('choose a new output directory; the demo never overwrites previous work')
    args.output_dir.parent.mkdir(parents=True,exist_ok=True)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start()
    source=args.output_dir.with_name(args.output_dir.name+'-manifest.csv')
    source.write_text('url\n'+''.join(f'http://127.0.0.1:{server.server_port}/{i}.png\n' for i in range(3)))
    try:return flowdc(['download','--input-path',str(source),'--output-dir',str(args.output_dir)])
    finally:server.shutdown();server.server_close()


if __name__=='__main__':
    raise SystemExit(main())
