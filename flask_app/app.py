from flask import Flask
from celery import Celery
from flask import url_for

app = Flask(__name__)
simple_app = Celery('simple_worker', broker='redis://redis:6379/0', backend='redis://redis:6379/0')
celery_app = Celery('module', broker='redis://redis:6379/0', backend='redis://redis:6379/0')


@app.route('/simple_start_task')
def call_method():
    app.logger.info("Invoking Method ")
    r = simple_app.send_task('tasks.longtime_add', kwargs={'x': 1, 'y': 2})
    app.logger.info(r.backend)
    return r.id


@app.route('/add/<int:param1>/<int:param2>')
def add(param1=9, param2=8):
    task = simple_app.send_task('tasks.longtime_add', kwargs={'x': param1, 'y': param2})
    line1 = f"<a href='{url_for('get_status', task_id=task.id, external=True)}'>Status: {task.id} </a><p></p>"
    line2 = f"<a href='{url_for('task_result', task_id=task.id, external=True)}'>Result: {task.id} </a><p></p>"
    r = line1 + line2
    print(r)
    #response = f"<a href='{url_for('get_status', task_id=task.id, external=True)}'>Status: {task.id} </a><p></p>"
    response = r
    return response

@app.route('/simple_task_status/<task_id>')
def get_status(task_id):
    status = simple_app.AsyncResult(task_id, app=simple_app)
    print("Invoking Method ")
    return "Status of the Task " + str(status.state)


@app.route('/simple_task_result/<task_id>')
def task_result(task_id):
    result = simple_app.AsyncResult(task_id).result
    return "Result of the Task " + str(result)


# Celery Module based Tasks

@app.route('/module_start_task')
def module_call_method():
    app.logger.info("Invoking Method ")
    app.logger.info(celery_app.tasks)
    r = celery_app.send_task('module.tasks.new_add', kwargs={'x': 1, 'y': 2}, queue='module')
    app.logger.info(r.backend)
    return r.id


@app.route('/module_task_status/<task_id>')
def module_get_status(task_id):
    status = celery_app.AsyncResult(task_id, app=celery_app)
    print("Invoking Method ")
    return "Status of the Task " + str(status.state)


@app.route('/module_task_result/<task_id>')
def module_task_result(task_id):
    result = celery_app.AsyncResult(task_id).result
    return "Result of the Task " + str(result)


@app.route('/')
def main(task_id):
    result =  str("Usages:" + 
                  "<p>/</p>" + 
                  "<p>/simple_start_task</p>" +
                  "<p>/simple_task_status/&lt;task_id&gt;</p>" + 
                  "<p>/simple_task_result/&lt;task_id&gt;</p>" + 
                  "<p>/module_start_task</p>" + 
                  "<p>/module_task_status/&lt;task_id&gt;</p>" +
                  "<p>/module_task_result/&lt;task_id&gt;</p>")
    return result