import hashlib
from sqlalchemy import Column, String, create_engine, Integer, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
import os
import matplotlib.pyplot as plt
from datetime import date
from boltons.timeutils import daterange
import numpy as np
import fitanalysis
import json

engine = create_engine('sqlite:///sports.db')
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()
Base = declarative_base(engine)


#
class Item(Base):
    __tablename__ = 'record_item'
    id = Column(Integer, autoincrement=True, primary_key=True)
    str_day = Column(String(32), unique=True)
    start_timestamp = Column(Integer)
    end_timestamp = Column(Integer)
    elapsed_time = Column(Float)
    moving_time = Column(Float)
    mean_power = Column(Float)
    norm_power = Column(Float)
    intensity = Column(Float)
    tss = Column(Float)
    atl = Column(Float)
    ctl = Column(Float)
    tsb = Column(Float)


class Fitfile(Base):
    __tablename__ = 'fit_file'
    id = Column(Integer, autoincrement=True, primary_key=True)
    file_md5 = Column(String(32), unique=True)
    file_name = Column(String(256))
    start_time = Column(String(32))
    end_time = Column(String(32))
    mean_power = Column(String(32))
    norm_power = Column(String(32))
    tss = Column(String(32))



def get_file_md5(fit_file):
    md5 = hashlib.md5()
    with open(fit_file, 'rb') as ff:
        while True:
            data = ff.read(4096)
            if not data: break
            md5.update(data)
    return md5.hexdigest()


def init_db():
    start = datetime(year=2022, month=1, day=1)
    end = datetime(year=2030, month=12, day=31)
    items = []
    for day in daterange(start, end):
        item = Item(str_day=day.strftime('%Y-%m-%d'),
                    start_timestamp=int(round(day.timestamp())),
                    elapsed_time=0,
                    moving_time=0,
                    end_timestamp=int(round(day.timestamp())),
                    mean_power=0,
                    norm_power=0,
                    intensity=0,
                    tss=0, atl=0, ctl=0, tsb=0
                    )
        items.append(item)
    session.add_all(items)
    session.commit()


def renew_db(FTP, CTL_DAYS, ATL_DAYS):
    # else:
    #     activities = {'files':[], 'data' : {}}
    activities = {}
    fit_items = []
    for fit_file in os.listdir('fits'):
        name, ext = os.path.splitext(fit_file)

        if ext.lower() == '.fit':
            md5 = get_file_md5(os.path.join('fits', fit_file))
            records = session.query(Fitfile).filter_by(file_md5=md5).all()

            if records:
                continue
            else:
                activity = fitanalysis.Activity('fits/%s' % fit_file)
                end_time = activity.end_time.strftime('%Y-%m-%d')
                if end_time in activities.keys():
                    activities[end_time]['tss'] = activities[end_time]['tss'] + activity.training_stress(FTP)
                else:
                    activities[end_time] = {'tss': activity.training_stress(FTP),
                                            'start_time': activity.start_time,
                                            'elapsed_time': float(activity.elapsed_time.total_seconds()),
                                            'moving_time': float(activity.moving_time.total_seconds()),
                                            'end_time': activity.end_time,
                                            'mean_power': float(activity.mean_power),
                                            'norm_power': float(activity.norm_power),
                                            'intensity': float(activity.intensity(FTP)),
                                            }
                fit_items.append(Fitfile(file_md5=md5, file_name=fit_file,
                                         start_time=activity.start_time.strftime('%Y-%m_%d %H:%M:%S'),
                                         end_time=activity.end_time.strftime('%Y-%m_%d %H:%M:%S'),
                                         mean_power= str(activity.mean_power),
                                         norm_power= str(activity.norm_power),
                                         tss= str(activity.training_stress(FTP))))
    if len(activities.keys()) < 1: return
    session.add_all(fit_items)
    session.commit()
    sorted_activities = dict(sorted(activities.items(), key=lambda v: v[0]))
    keys = list(sorted_activities.keys())
    pass_day = int(round((sorted_activities[keys[0]]['end_time'] + timedelta(days=-CTL_DAYS)).timestamp()))
    this_day = int(round(sorted_activities[keys[-1]]['end_time'].timestamp()))
    records = session.query(Item).filter(Item.end_timestamp >= pass_day).filter(Item.end_timestamp <= this_day) \
        .order_by(Item.str_day).all()
    tss_array = np.zeros((len(records)), dtype=Float)
    find = False
    new_records = []
    for i, record in enumerate(records):
        if record.str_day in keys:
            act = activities[record.str_day]
            record.tss = record.tss + act['tss']
            tss_array[i] = record.tss
            find = True

        else:
            tss_array[i] = record.tss
        if find:
            record.ctl = np.sum(tss_array[i - CTL_DAYS + 1:i + 1]) / CTL_DAYS
            record.atl = np.sum(tss_array[i - ATL_DAYS + 1:i + 1]) / ATL_DAYS
            record.tsb = record.ctl - record.atl

        session.commit()


def draw_power_plot(PLOT_DAYS):
    today = date.today()
    pass_day = (today + timedelta(days=-PLOT_DAYS)).strftime('%Y-%m_%d %H:%M:%S')
    activities = session.query(Fitfile).filter(Fitfile.end_time >= pass_day).order_by(Fitfile.end_time).all()
    x = []
    mean_power = []
    norm_power = []
    tss = []
    for item in activities:
        end_time = item.end_time.split()[0]

        x.append(end_time)
        mean_power.append(float(item.mean_power))
        norm_power.append(float(item.norm_power))
        tss.append(float(item.tss))

    plt.plot(x, mean_power, 'o-', label='Mean Power')
    plt.plot(x, norm_power, 'o-', label='Norm Power')
    plt.plot(x, tss, 'o-', label='TSS')
    plt.grid()
    plt.legend()
    plt.title('Power Plot')
    plt.xticks(rotation=90)
    plt.show()


def draw_date_tss(PLOT_DAYS):
    today = datetime.today()
    pass_day = int(round((today + timedelta(days=-PLOT_DAYS)).timestamp()))
    future_day = int(round((today + timedelta(days=0)).timestamp()))
    x = []
    tss = []
    atl = []
    ctl = []
    tsb = []

    activities = session.query(Item).filter(Item.end_timestamp <= future_day).filter \
        (Item.end_timestamp >= pass_day).all()
    for item in activities:
        x.append(item.str_day)
        tss.append(item.tss)
        atl.append(item.atl)
        ctl.append(item.ctl)
        tsb.append(item.tsb)
    plt.plot(x, tss, 'o-', label='TSS')
    plt.plot(x, atl, 'o-', label='ATL')
    plt.plot(x, tsb, 'o-', label='TSB')
    plt.plot(x, ctl, 'o-', label='CTL')
    plt.grid()
    plt.legend()
    plt.title('Trainning Effect')
    plt.xticks(rotation=90)
    plt.show()


if __name__ == '__main__':

    if not os.path.exists('config.json'):
        print('There is no config file, will set FTP=200, CTL_DAYS=42, ATL_DAYS=7')
        config = {"FTP": 200,"CTL_DAYS": 42,"ATL_DAYS": 7, "PLOT_DAYS":30}
        with open('config.json', 'w') as cf:
            cf.write(json.dumps(config))
    else:
        with open('config.json', 'r') as cf:
            config = json.loads(cf.read())

    if not os.path.exists('sports.db'):
        Base.metadata.drop_all()
        Base.metadata.create_all()
        init_db()
    # # draw_date_tss()
    renew_db(config['FTP'],config['CTL_DAYS'],config['ATL_DAYS'])
    draw_date_tss(config['PLOT_DAYS'])
    draw_power_plot(config['PLOT_DAYS'])
