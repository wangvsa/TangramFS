#!/usr/bin/env python
# encoding: utf-8
import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets
from torchvision.transforms import ToTensor, Lambda, Compose

from typing import Any, Callable, Dict, List, Optional, Tuple
from PIL import Image
import os, io, ctypes


class MyFlickr8kDataset(datasets.VisionDataset):
    def __init__(
        self,
        root: str,
        ann_file: str,
        libname: str,
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None
    ) -> None:
        super().__init__(root, transform=transform, target_transform=target_transform)
        self.root = root
        self.ann_file = os.path.expanduser(ann_file)
        self.annotations: Dict[str, List[str]] = {}

        self.parse_annotation_file()
        self.ids = list(sorted(self.annotations.keys()))
        self.libtangram = ctypes.CDLL(libname)
        self.libtangram.tfs_fetch.restype  = ctypes.c_size_t
        self.libtangram.tfs_fetch.argtypes = [ctypes.c_char_p, ctypes.c_void_p]
        self.libtangram.tfs_init()

        self.buffer = ctypes.create_string_buffer(3*600*600)
        self.buffer = ctypes.cast(self.buffer, ctypes.c_void_p);

    def destroy(self):
        self.libtangram.tfs_finalize()

    def parse_annotation_file(self):
        with open(self.ann_file) as fh:
            for line in fh.readlines():
                data = line.split(",")
                img_id, caption = data[0], data[1]
                img_id = os.path.join(self.root, img_id)
                if ".jpg" not in img_id: continue
                if img_id not in self.annotations:
                    self.annotations[img_id] = []
                self.annotations[img_id].append(caption.strip())

    def __getitem__(self, index: int) -> Tuple[Any, Any]:
        img_id = self.ids[index]
        img_size = os.path.getsize(img_id)

        buf = ctypes.create_string_buffer(img_size)
        img_size = self.libtangram.tfs_fetch(ctypes.c_char_p(str.encode(img_id)), buf)
        img = Image.open(io.BytesIO( buf ))

        #img = Image.open(img_id)

        # Make sure each image is of the same size
        # so they can be loaded into batches
        img = img.convert("RGB")
        img = img.crop((0, 0, 30, 30))
        if self.transform is not None:
            img = self.transform(img)

        # Captions
        target = self.annotations[img_id]
        if self.target_transform is not None:
            target = self.target_transform(target)

        #return img, target
        return img, 0

    def __len__(self) -> int:
        return 10
        #return len(self.ids)


def get_flickr8k_dataloader(root = "data/Flickr8k"):

    # Download training data from open datasets.
    training_data = MyFlickr8kDataset(
        root=root+"/Images",
        ann_file=root+"/captions.txt",
        libname="/home/wangchen/Sources/TangramFS/install/lib/libtangramfs.so",
        transform=ToTensor(),
    )

    # Create data loaders.
    batch_size = 64
    train_dataloader = DataLoader(training_data, batch_size=batch_size)

    #for X, y in train_dataloader:
    #    print("Shape of X [N, C, H, W]: ", X.shape)
    #    print("Shape of y: ", y.shape, y.dtype)
    #    break

    return train_dataloader


train_dataloader = get_flickr8k_dataloader()


# Get cpu or gpu device for training.
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using {device} device")

# Define model
class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(3*30*30, 512),
            nn.ReLU(),
            nn.Linear(512, 512),
            nn.ReLU(),
            nn.Linear(512, 10)
        )

    def forward(self, x):
        x = self.flatten(x)
        logits = self.linear_relu_stack(x)
        return logits

model = NeuralNetwork().to(device)

print(model)
loss_fn = nn.CrossEntropyLoss()
optimizer = torch.optim.SGD(model.parameters(), lr=1e-3)

def train(dataloader, model, loss_fn, optimizer):
    size = len(dataloader.dataset)
    model.train()
    for batch, (X, y) in enumerate(dataloader):
        X, y = X.to(device), y.to(device)

        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch % 20 == 0:
            loss, current = loss.item(), batch * len(X)
            print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


epochs = 2
for t in range(epochs):
    print(f"Epoch {t+1}\n-------------------------------")
    train(train_dataloader, model, loss_fn, optimizer)
print("Done!")
