import argparse
import socket
import json

def __json_bytes(obj):
    return (json.dumps(obj) + "\n").encode("UTF-8")

def __connect(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", port))
    return s

def send_model_load(path, port):
    with __connect(port) as s:
        s.sendall(__json_bytes({"type": "load model"}))
        s.sendall(__json_bytes({"path": path}))
        s.sendall(__json_bytes({"final": True}))

def dump_model_experience(port, bao_db):
    with __connect(port) as s:
        s.sendall(__json_bytes({"type": "clear model"}))
        s.sendall(__json_bytes({"bao_db": bao_db}))
        s.sendall(__json_bytes({"final": True}))

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Bao for PostgreSQL Controller")
    parser.add_argument("--retrain", action="store_true",
                        help="Force the Bao server to train a model and load it")

    parser.add_argument("--clear", action="store_true")

    parser.add_argument("--bao-model-path", type=str)
    parser.add_argument("--bao-old-model-path", type=str)
    parser.add_argument("--bao-tmp-model-path", type=str)

    parser.add_argument("--bao-db", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    
    args = parser.parse_args()

    if args.retrain:
        assert args.bao_model_path
        assert args.bao_old_model_path
        assert args.bao_tmp_model_path

        import train
        train.train_and_swap(
            args.bao_model_path,
            args.bao_old_model_path,
            args.bao_tmp_model_path,
            args.bao_db,
            verbose=True)
        send_model_load(args.bao_model_path, args.port)
        exit(0)

    elif args.clear:
        dump_model_experience(args.port, args.bao_db)
        exit(0)
