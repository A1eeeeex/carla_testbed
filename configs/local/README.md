# configs/local/

Machine-local override templates.

Files in this directory are for local paths, hostnames, ports, container names, and output roots. Template files may be committed; real machine overrides should stay private.

Start from `local.example.yaml` and copy it to a private filename such as:

```text
configs/local/local.yaml
configs/local/<hostname>.yaml
```

Do not commit real CARLA, Apollo, map, Docker, or output paths.
