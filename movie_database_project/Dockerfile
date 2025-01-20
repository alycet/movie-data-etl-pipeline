FROM public.ecr.aws/lambda/python:3.10
#FROM public.ecr.aws/amazonlinux/amazonlinux:latest
#FROM python:3.10

# Install chrome dependencies
RUN yum install -y atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel nss mesa-libgbm jq unzip


# Copy and run chrome installer scripts
COPY chrome-installer.sh .
RUN /usr/bin/bash chrome-installer.sh


# Copy and install pip requirements
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy utility funtions
COPY utils.py .

# Copy functions 
COPY data_extract.py .
COPY data_transformation.py .

# Command to run function
#CMD ["data_extract.handler"]


