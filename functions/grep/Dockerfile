FROM registry.gitlab.polimi.it/ai-sprint/toscarizer/ai-sprint-base

RUN mkdir /opt/videosearcher
RUN mkdir /mnt/ramdisk

WORKDIR /opt/videosearcher

# Copy requirements files
COPY ./functions/grep/requirements.* /opt/videosearcher/

# Instal system requirements first
# Debian 11 left the main mirrors: its security packages are on archive.debian.org.
RUN touch requirements.sys && \
    find /etc/apt \( -name '*.list' -o -name '*.sources' \) | xargs sed -i 's|deb.debian.org/debian-security|archive.debian.org/debian-security|g' && \
    apt update && \
    xargs apt install -y --no-install-recommends < requirements.sys  && \
    apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && rm -rf ~/.cache/

# Update pip and install python requirements
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy application and onnx models
COPY ./functions/grep/ /opt/videosearcher/
COPY ./common/*.py /opt/videosearcher/

CMD ["python3", "/opt/videosearcher/function.py"]
