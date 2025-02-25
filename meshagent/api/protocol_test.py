import pytest
import asyncio
import logging

from meshagent.api.protocol import MemoryServerProtocol, MemoryClientProtocol
from meshagent.api.chan import Chan

logger = logging.getLogger("test")
logger.setLevel(logging.INFO)

@pytest.mark.asyncio
async def test_protocol():
    
    input = Chan[bytes]()
    output = Chan[bytes]()
      
    async with MemoryClientProtocol(input=input, output=output) as client:

      async with MemoryServerProtocol(input=output, output=input, token="") as server:
          
          last_data = None
          last_type = None
          last_message_id = None

          fut = asyncio.Future()

          def handler(_, message_id, type, data):
              nonlocal last_data
              nonlocal last_type
              nonlocal last_message_id

              last_data = data
              last_type = type
              last_message_id = message_id

              fut.set_result(True)

              
            
          server.register_handler("hello", handler)
      
          size = 1000 * 100

          def yield_bytes():
              for i in range(size):
                  yield 1

          data = bytes(iter(yield_bytes()))

          await client.send(type="hello",data=data, message_id=2)

          await fut

          assert last_data != None
          assert last_type == "hello"
          assert last_message_id == 2

          for i in range(size):
            assert last_data[i] == 1
          
       
