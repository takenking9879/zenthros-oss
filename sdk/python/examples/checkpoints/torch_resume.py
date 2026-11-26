# SPDX-License-Identifier: Apache-2.0

"""A PyTorch loop that resumes where a lost machine stopped.

The weights and the optimizer are one file of the checkpoint, saved at the
end of each epoch and at once on the interruption notice. The replacement
loads them and runs the epochs left.

    weights = zenthros.run(train, make_model, batches, epochs=20, on="gpu-spot")

make_model() builds the module; batches() yields (x, y) for one epoch.
"""

import io

import zenthros

WEIGHTS = "weights.pt"


def train(make_model, batches, epochs, lr=1e-3, phase="torch"):
    import torch

    model = make_model()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    ckpt = zenthros.Checkpointer(phase)
    policy = zenthros.CheckpointPolicy(every=1)
    start = 0
    if ckpt.state:
        saved = torch.load(ckpt.file_path(WEIGHTS), weights_only=True)
        model.load_state_dict(saved["model"])
        optimizer.load_state_dict(saved["optimizer"])
        start = ckpt.state["epoch"] + 1
    loss_fn = torch.nn.MSELoss()
    for epoch in range(start, epochs):
        for x, y in batches():
            optimizer.zero_grad()
            loss_fn(model(x), y).backward()
            optimizer.step()
        if policy.due(ckpt, epoch, epochs):
            buf = io.BytesIO()
            torch.save({"model": model.state_dict(), "optimizer": optimizer.state_dict()}, buf)
            ckpt.save({"epoch": epoch}, files={WEIGHTS: buf.getvalue()}, complete=epoch + 1 >= epochs)
    return model.state_dict()
