FROM amd64/centos:7

# This must be lower case - used in the paths and packages names
ARG TARGET

# Install tools for cross compiling if the requested target is different from currenly running image and
RUN yum -y update \
    && yum install -y git gcc make curl libffi-devel dnf epel-release ca-certificates \
    perl-core openssl openssl-devel pkgconfig libatomic \
    && if [ "$(uname -m)" != ${TARGET} ]; then \
    yum install -y "binutils-${TARGET}-linux-gnu" "gcc-${TARGET}-linux-gnu" "gcc-c++-${TARGET}-linux-gnu" \
    && if [ ${TARGET} == "aarch64" ]; then \
    curl -L -s https://www.centos.org/keys/RPM-GPG-KEY-CentOS-7-aarch64 > /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7-aarch64 \
    && cat /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7-aarch64 >> /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 ; \
    fi \
    && dnf --forcearch "${TARGET}" --release 7 install -y gcc glibc glibc-devel --installroot "/usr/${TARGET}-linux-gnu/sys-root/" || true \
    && ln -s "/usr/${TARGET}-linux-gnu/sys-root/usr/lib64/libgcc_s.so.1" "/usr/${TARGET}-linux-gnu/sys-root/usr/lib64/libgcc_s.so"; \
    fi \
    && yum clean all \
    && rm -rf /var/cache/yum

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal \
    && rustup target add "${TARGET}-unknown-linux-gnu" \
    # Reduce memory consumption by avoiding cargo's libgit2
    && echo -e "[net]\ngit-fetch-with-cli = true" > $CARGO_HOME/config
