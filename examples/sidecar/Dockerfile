FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-runtime
RUN pip install --no-cache-dir -U transformers accelerate peft google-cloud-storage huggingface_hub
# Bake the weights. An immutable dependency belongs in the layer: it pins the
# exact model by image digest, survives a hub outage or rate limit, and needs
# no external egress at run time. Checkpoints go to object storage instead,
# because those are mutable artifacts that must outlive the pod.
RUN python -c "\
from huggingface_hub import snapshot_download; \
snapshot_download('google/gemma-4-E2B-it', local_dir='/models/gemma', \
                  ignore_patterns=['*.pth','*.gguf','original/*'])" \
 && du -sh /models/gemma
COPY serve.py /work/
WORKDIR /work
