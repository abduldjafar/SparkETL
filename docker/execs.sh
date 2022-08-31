#Online Bash Shell.
#Code, Compile, Run and Debug Bash script online.
#Write your code in this editor and press "Run" button to execute it.


while true; do ping -c1 ${MYSQL_HOST} > /dev/null \
  && ls -alh \
  && mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} --password=${MYSQL_PASSWORD} < employees.sql \
  && echo "finish load employees db" \
  && mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} --password=${MYSQL_PASSWORD} < sakila/sakila-mv-schema.sql \
  && mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} --password=${MYSQL_PASSWORD} < sakila/sakila-mv-data.sql \
  && echo "finish load sakila db" \
  && break; done