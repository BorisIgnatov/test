from flask import Flask, json, request
from flask_cors import CORS
from flask_mail import Mail, Message

app = Flask(__name__)
CORS(app)

app.config['MAIL_SERVER']='ala-server05.alhilalbank.ae'
app.config['MAIL_PORT'] = 25
app.config['MAIL_USERNAME'] = 'bpm@alhilalbank.kz'
app.config['MAIL_PASSWORD'] = ''
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True

mail = Mail(app)

authorizer_email = 'atxliwukdijqxkhzql@niwghx.com'
complience_email = 'xbpptzadtkyytdcibd@twzhhq.online'
operational_email = 'xbpptzadtkyytdcibd@twzhhq.online'

def worker(task_id, topic_index):
    url_fetch = 'http://78.140.223.50:8080/engine-rest/external-task/fetchAndLock'
    base_url_execute = 'http://78.140.223.50:8080/engine-rest/external-task/'
    topic_name = ['send_compl_risk_topic', 'send_authorizer_t24_topic',
                    'send_notification_authorization_topic', 'send_to_operational_topic']
    workerId = "workerId"
    header = {'Content-Type':'application/json'}
    fetch_body = {
      "workerId":"workerId",
      "maxTasks":1,
      "usePriority":True,
      "topics":
          [{"topicName": topic_name[topic_index],
          "lockDuration": 10000
          }]
    }

    req_fetch = requests.post(url_fetch, data = json.dumps(fetch_body), headers = header)

    if req_fetch.status_code == '200':
        req_exe = requests.post(base_url_execute + task_id, data = json.dumps(workerId), headers = header)

    if req_fetch == '200' and req_exe.status_code == '204':
        return True
    return False


@app.route("/complience_risk_send")
def complience_risk():
    task_id = request.form.get('task_id')
    text = "Проверьте риск"
    msg = Message('Проеврка рисков', sender = app.config['MAIL_USERNAME'], recipients = [authorizer_email])
    msg.body = text
    mail.send(msg)
    return worker(task_id, 0)


@app.route("/notification_authorizer")
def notification_authorizer():
    task_id = request.form.get('task_id')
    text = "К Вам поступила новая задача. Для обработки задачи просим пройти по ссылке\n"
    msg = Message('Новая задача', sender = app.config['MAIL_USERNAME'], recipients = [authorizer_email])
    msg.body = text
    mail.send(msg)
    return worker(task_id, 1)


@app.route("/notification_authorized")
def notification_authorized():
    task_id = request.form.get('task_id')
    text = "Заявка клиента авторизована"
    msg = Message('Заявка авторизована', sender = app.config['MAIL_USERNAME'], recipients = [authorizer_email])
    msg.body = text
    mail.send(msg)
    return worker(task_id, 2)


@app.route("/operational_send")
def operational_send():
    task_id = request.form.get('task_id')
    text = "Новая заявка"
    msg = Message('Заявка нового клиента', sender = app.config['MAIL_USERNAME'], recipients = [operational_email])
    msg.body = text
    mail.send(msg)
    return worker(task_id, 3)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)
