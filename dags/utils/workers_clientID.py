import time
import logging
from worker_refresh_token import get_latest_ac_token, get_latest_refresh_token, request_new_ac_token_refresh_token


def get_workers() -> dict : 

    workers  = {
    "worker1": {"client_id": "c6c8f465c2d04e83b1a4df4e291ab7e4", "client_secret": "a62d93d1a43a420f8db1214c5d3b6525"},
    "worker2": {"client_id": "338fb263b36446ae995644dffd71e90a", "client_secret": "e7b130133a6549248bbe45beb77bd873"},
    "worker3": {"client_id": "c80c45ec9ef44973a69110fe1a8d6a49", "client_secret": "6c9c7c7329d346f989a150e515109800"},
    "worker4": {"client_id": "5d74f47b9b4a47409290e76895c6dc5c", "client_secret": "a317197c42d547d19f5633b99e9291f9"},
    "worker5": {"client_id": "0e544631eee04591b32066e73c7c165f", "client_secret": "b662d29617f44474832d7c3f08fa679b"},
    "worker6": {"client_id": "5c21b256144d4ab9ae0aad7a54395304", "client_secret": "2d243618ab72493094d24c6c1073acc3"},
    "worker7": {"client_id": "67582a48c7e640de802ff8f1dc6faa4c", "client_secret": "e86094049a3340d3acda9bea6cf0a825"},
    "worker8": {"client_id": "51ee97c3634d49a6bec83fbd22973e8b", "client_secret": "6770be92450f477e99488b2ddeb8ac1d"},
    "worker9": {"client_id": "71328da87c41450fa46d97c7482df1f4", "client_secret": "cc60d88fcdbc4f95a72a25fbca90fd79"},
    "worker10": {"client_id": "cfc9d87588e443cbab833208c17dd73c", "client_secret": "56c4d80ed23f45d0a2a3c79cf3cfa18c"}
}
    return workers

