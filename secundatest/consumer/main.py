from faststream import FastStream
from faststream.annotations import FastStream

from secundatest.broker.broker import broker
from secundatest.consumer import consumer


app = FastStream(broker)