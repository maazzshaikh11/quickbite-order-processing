#!/bin/bash
# Full clean restart of the QuickBite system (API + 4 workers) with a fresh DB
# and purged queues. Safe to run repeatedly.
set -u
cd ~/workspace/quickbite-order-processing

# Stop existing processes (match by full command line; never match this script)
python3 -c "
import os, signal, subprocess
me = os.getpid()
out = subprocess.run(['ps','-eo','pid,ppid,args'],capture_output=True,text=True).stdout
mine = {me}
for line in out.splitlines()[1:]:
    parts = line.split(None, 2)
    if len(parts) < 3: continue
    pid, ppid, args = int(parts[0]), int(parts[1]), parts[2]
    if 'quickbite.api.main' in args or 'quickbite.workers.' in args:
        # don't kill ourselves or our ancestors
        p, chain = pid, set()
        while p and p not in chain:
            chain.add(p); p = int(subprocess.run(['ps','-o','ppid=','-p',str(p)],capture_output=True,text=True).stdout.strip() or 0)
        if me not in chain:
            print('killing', pid, args[:60]); os.kill(pid, signal.SIGKILL)
"
sleep 1

PGPASSWORD=quickbite psql -h localhost -U quickbite -d quickbite \
  -c "DROP TABLE IF EXISTS processed_events, notifications, orders;" > /dev/null
for q in payment.queue restaurant.queue delivery.queue notification.queue \
         orders.dead-letter.queue payment.queue.retry restaurant.queue.retry \
         delivery.queue.retry notification.queue.retry; do
  curl -s -u guest:guest -X DELETE \
    "http://localhost:15672/api/queues/%2F/$q/contents" > /dev/null
done

export DATABASE_URL="postgresql+asyncpg://quickbite:quickbite@localhost:5432/quickbite"
export RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
export PAYMENT_MIN_DELAY_SECONDS=0.2 PAYMENT_MAX_DELAY_SECONDS=0.5
export RESTAURANT_DELAY_SECONDS=0.3 DELIVERY_DELAY_SECONDS=0.3
export POSTGRES_USER=quickbite POSTGRES_PASSWORD=quickbite POSTGRES_DB=quickbite

nohup .venv/bin/python -m quickbite.api.main > /tmp/qb-logs/api.log 2>&1 &
nohup .venv/bin/python -m quickbite.workers.payment > /tmp/qb-logs/payment.log 2>&1 &
nohup .venv/bin/python -m quickbite.workers.restaurant > /tmp/qb-logs/restaurant.log 2>&1 &
nohup .venv/bin/python -m quickbite.workers.delivery > /tmp/qb-logs/delivery.log 2>&1 &
nohup .venv/bin/python -m quickbite.workers.notification > /tmp/qb-logs/notification.log 2>&1 &
sleep 14
curl -s http://localhost:8000/health; echo
