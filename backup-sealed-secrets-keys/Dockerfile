# Creator  David Darville - bw7
# Start with Ubuntu LTS 22.04
FROM ubuntu:jammy

RUN apt-get update
# Otherwise, the install requires keyboard interaction
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata
RUN apt-get install -y jq gzip gnupg wget apt-transport-https ca-certificates unzip bind9-dnsutils vim less dateutils curl libdbd-pg-perl libdata-dumper-simple-perl inotify-tools --no-install-recommends
RUN apt-get dist-upgrade -y

# Enable Google Kubernetes repo
RUN wget --quiet -O - https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" > /etc/apt/sources.list.d/kubernetes.list

RUN apt-get update
RUN apt-get install -y kubectl --no-install-recommends

# Download and install AWS CLI v2
RUN wget --quiet https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip
RUN unzip awscli-exe-linux-x86_64.zip
RUN aws/install
RUN rm -fr aws/ awscli-exe-linux-x86_64.zip

# Remove stuff that is now unneeded
RUN apt-get purge -v gnupg apt-transport-https ca-certificates unzip
RUN apt-get autoremove --yes
RUN apt-get clean
RUN rm -fr /var/cache /var/lib/apt

# Create user account
RUN echo "script:x:1000:1000::/home/script:/bin/bash" >> /etc/passwd
RUN echo "script:x:1000:" >> /etc/group
RUN mkdir /home/script
RUN chown 1000:1000 /home/script
USER 1000:1000

CMD ["/bin/bash", "/mnt/script"]