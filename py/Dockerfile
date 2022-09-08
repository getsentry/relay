FROM centos:7

# This must be lower case - used in the paths and packages names
ARG TARGET

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# Rust toolchain
RUN yum install -y --nogpgcheck git gcc make curl libffi-devel \
  && curl https://sh.rustup.rs -sSf | sh -s -- -y \
  && source ${CARGO_HOME}/env \
  && rustup target add "${TARGET}-unknown-linux-gnu" \
  # Reduce memory consumption by avoiding cargo's libgit2
  && echo -e "[net]\ngit-fetch-with-cli = true" > $CARGO_HOME/config

# Install tools for cross compiling if the requested target is different from currenly running image and
# also threat the i686 a little bit special, since we must configure addition things and install different set of libraries
RUN echo "Setup the build environment for target: ${TARGET}" \
  && if [ ${TARGET} != "i686" ] && [ "$(uname -m)" != ${TARGET} ]; then \
    yum install -y dnf epel-release \
    && yum install --nogpgcheck -y "binutils-${TARGET}-linux-gnu" "gcc-${TARGET}-linux-gnu" "gcc-c++-${TARGET}-linux-gnu" \
    && dnf --nogpgcheck --forcearch "${TARGET}" --release 7 install -y gcc glibc glibc-devel --installroot "/usr/${TARGET}-linux-gnu/sys-root/" || true \
    && ln -s "/usr/${TARGET}-linux-gnu/sys-root/usr/lib64/libgcc_s.so.1" "/usr/${TARGET}-linux-gnu/sys-root/usr/lib64/libgcc_s.so"; \
  elif [ ${TARGET} == "i686" ]; then \
    yum install -t -y --nogpgcheck glibc-devel.i686 libgcc.i686 \
    && RG=$(find /opt -type f -executable -name gcc) \
    && if ! [ -z $RG ]; then ln -s $RG /usr/bin/i686-linux-gnu-gcc; else ln -s /usr/bin/gcc /usr/bin/i686-linux-gnu-gcc; fi; \
  fi \
  && yum clean all \
  && rm -rf /var/cache/yum
