import argparse
import socketserver
import json
import struct
import sys
import time
import os
import storage
import model
import train
import baoctl
import math
from constants import (PG_OPTIMIZER_INDEX)

def add_buffer_info_to_plans(buffer_info, plans):
    for p in plans:
        p["Buffers"] = buffer_info
    return plans

class BaoModel:
    def __init__(self):
        self.__current_model = None

    def select_plan(self, messages):
        start = time.time()
        # the last message is the buffer state
        *arms, buffers = messages

        # if we don't have a model, default to the PG optimizer
        if self.__current_model is None:
            return PG_OPTIMIZER_INDEX

        # if we do have a model, make predictions for each plan.
        arms = add_buffer_info_to_plans(buffers, arms)
        res = self.__current_model.predict(arms)
        idx = res.argmin()
        stop = time.time()
        print("Selected index", idx,
              "after", f"{round((stop - start) * 1000)}ms",
              "Predicted reward / PG:", res[idx][0],
              "/", res[0][0])
        return idx

    def predict(self, messages):
        # the last message is the buffer state
        plan, buffers = messages

        # if we don't have a model, make a prediction of NaN
        if self.__current_model is None:
            return math.nan

        # if we do have a model, make predictions for each plan.
        plans = add_buffer_info_to_plans(buffers, [plan])
        res = self.__current_model.predict(plans)
        return res[0][0]
    
    def load_model(self, fp):
        try:
            new_model = model.BaoRegression(have_cache_data=True)
            new_model.load(fp)

            self.__current_model = new_model
            print("Accepted new model.")

        except Exception as e:
            print("Failed to load Bao model from", fp,
                  "Exception:", sys.exc_info()[0])
            raise e

    def clear_model(self, bao_db):
        self.__current_model = None
        storage.clear_experience(bao_db)
        print("Cleared model.")
            

class JSONTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        str_buf = ""
        while True:
            str_buf += self.request.recv(1024).decode("UTF-8")
            if not str_buf:
                # no more data, connection is finished.
                return
            
            if (null_loc := str_buf.find("\n")) != -1:
                json_msg = str_buf[:null_loc].strip()
                str_buf = str_buf[null_loc + 1:]
                if json_msg:
                    try:
                        if self.handle_json(json.loads(json_msg)):
                            break
                    except json.decoder.JSONDecodeError:
                        print("Error decoding JSON:", json_msg)
                        break


class BaoJSONHandler(JSONTCPHandler):
    def setup(self):
        self.__messages = []
    
    def handle_json(self, data):
        if "final" in data:
            message_type = self.__messages[0]["type"]
            self.__messages = self.__messages[1:]

            if message_type == "query":
                result = self.server.bao_model.select_plan(self.__messages)
                self.request.sendall(struct.pack("I", result))
                self.request.close()
            elif message_type == "predict":
                result = self.server.bao_model.predict(self.__messages)
                self.request.sendall(struct.pack("d", result))
                self.request.close()
            elif message_type == "reward":
                plan, buffers, obs_reward, bao_db = self.__messages
                plan = add_buffer_info_to_plans(buffers, [plan])[0]
                storage.record_reward(bao_db["bao_db"], plan, obs_reward["reward"], obs_reward["pid"])
            elif message_type == "load model":
                path = self.__messages[0]["path"]
                self.server.bao_model.load_model(path)
            elif message_type == "clear model":
                bao_db = self.__messages[0]["bao_db"]
                self.server.bao_model.clear_model(bao_db)
            else:
                print("Unknown message type:", message_type)
            
            return True

        self.__messages.append(data)
        return False
                

def start_server(listen_on, port):
    model = BaoModel()

    #if os.path.exists(DEFAULT_MODEL_PATH):
    #    print("Loading existing model")
    #    model.load_model(DEFAULT_MODEL_PATH)
    
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer((listen_on, port), BaoJSONHandler) as server:
        server.bao_model = model
        server.serve_forever()


if __name__ == "__main__":
    from multiprocessing import Process

    parser = argparse.ArgumentParser(prog="Process")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    port = int(args.port)
    listen_on = "localhost"

    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex((listen_on, port))
    if result == 0:
        # Don't double-load
        exit(0)
    sock.close()

    print(f"Listening on {listen_on} port {port}")
    server = Process(target=start_server, args=[listen_on, port])
    
    print("Spawning server process...")
    server.start()
