import os

from polygon import RESTClient

class PolygonHelper():
    """A helper class to abstract common polygon functionality."""
    def __init__(self, polygon_token: str = None) -> None:
        # if token not passed, is it in env
        polygon_token = polygon_token if polygon_token is not None else os.getenv('polygon_key')
        if not polygon_token:
            raise Exception("No polgygon token found.")
        self.client = RESTClient(api_key=polygon_token)
