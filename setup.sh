conf_file_path=/etc/bs_bigdata_etl.conf

python setup.py install \
  && cp bs_bigdata_etl.service /lib/systemd/system \
  && if [ ! -f "$conf_file_path" ]; then cp etc/bs_bigdata_etl.conf /etc/; fi \
  && echo "Setup successfully"

