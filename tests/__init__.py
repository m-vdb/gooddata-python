import argparse


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--loglevel', dest='loglevel',
        help='Set loglevel'
    )

    return parser
