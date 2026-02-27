__copyright__   = "Copyright 2026, VISA Lab"
__license__     = "MIT"

import torch
import io
from PIL import Image
from facenet_pytorch import MTCNN, InceptionResnetV1

data_path = 'model/data.pt'

mtcnn = MTCNN(image_size=240, margin=0, min_face_size=20) # initializing mtcnn for face detection
resnet = InceptionResnetV1(pretrained='vggface2').eval() # initializing resnet for face img to embeding conversion

saved_data = torch.load(data_path) # loading data.pt file
embedding_list = saved_data[0] # getting embedding data
name_list = saved_data[1] # getting list of names

def face_match(img_path): # img_path= location of photo, data_path= location of data.pt
    # getting embedding matrix of the given img
    if isinstance(img_path, str):
        img = Image.open(img_path).convert('RGB')
    elif isinstance(img_path, bytes):
        img = Image.open(io.BytesIO(img_path)).convert('RGB')
    else:
        raise ValueError("image_input must be str path or bytes")
    face, prob = mtcnn(img, return_prob=True) # returns cropped face and probability
    if face is None:
        return "Unknown"
    emb = resnet(face.unsqueeze(0)).detach() # detech is to make required gradient false

    dist_list = [] # list of matched distances, minimum distance is used to identify the person

    for idx, emb_db in enumerate(embedding_list):
        dist = torch.dist(emb, emb_db).item()
        dist_list.append(dist)

    idx_min = dist_list.index(min(dist_list))
    return name_list[idx_min]

if __name__ == "__main__":
    import sys

    image_path = sys.argv[1]

    result = face_match(image_path)
    print(result)