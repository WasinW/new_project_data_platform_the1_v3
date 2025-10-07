# dataflow_common_base

This directory contains the source code of the generic `dataflow_common` library
and a Dockerfile for building a base image.  The base image copies the
`dataflow_common` package into `/opt/dataflow_common` and installs the
dependencies listed in `requirements.txt`.  It is intended for use as a
build stage in pipeline images rather than being executed directly.

The Google Cloud Dataflow Python runner stages only the pipeline script
and the modules in its local directory into a virtual environment
on the worker.  Therefore a pipeline image must copy the `dataflow_common`
package into the same directory as the pipeline script.  To avoid
repeatedly copying the library from the repository, you can build this
base image once and then reference it in multi‑stage builds for your
pipeline images.

## Building the base image

```
docker build -t <REGION>-docker.pkg.dev/<PROJECT>/<REPOSITORY>/dataflow-common-base:<TAG> .
docker push <REGION>-docker.pkg.dev/<PROJECT>/<REPOSITORY>/dataflow-common-base:<TAG>
```

Replace `<REGION>`, `<PROJECT>`, `<REPOSITORY>`, and `<TAG>` with the
appropriate values for your environment.  See the `README.md` in the
pipeline directory for an example of using this base image.