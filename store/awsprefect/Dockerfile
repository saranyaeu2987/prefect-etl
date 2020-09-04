FROM 202991147671.dkr.ecr.us-east-1.amazonaws.com/standalone-spark-cluster:2.2.0


# Pull over code and get environment set up
RUN mkdir /usr/store
COPY . /usr/store/

# Get pip set up
RUN pip install pip==18.1
RUN pip3 install -r /usr/store/app/requirements.txt
RUN pip3 install wheel

# Set working directory
WORKDIR /usr/store/
RUN mkdir data_storage

ENV PYTHONPATH "${PYTHONPATH}:/usr/store"
