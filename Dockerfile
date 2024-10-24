FROM public.ecr.aws/lambda/python:3.9
COPY app.py /var/task
COPY fn.py /var/task
RUN  pip3 install pandas --target /var/task
RUN pip install openpyxl --target /var/task
RUN pip install xlsxwriter --target /var/task
CMD ["app.lambda_handler"]