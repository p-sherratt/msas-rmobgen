#!/usr/bin/env python3

import re
import time
import ftplib
import datetime
import argparse
import yaml
from collections import defaultdict
from calendar import monthrange
from PIL import Image, ImageDraw, ImageFont
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


# todo - import logging and show it some <s>hate</s>love.

def rmob_main(args):
    config = RmobConfig(args.config_file)

    if args.watch:
        observer = Observer()
        handler = FileChangeHandler(lambda: rmob_export(config, args.upload, args.denoise, None))
        observer.schedule(handler, config.datapath)
        observer.start()
        month = None
        try:
            while True:
                today = datetime.date.today()
                if month != today:
                    month = today
                    month_str = month.strftime("%Y%m")
                    handler.set_file_path("{}/RMOB-{}.dat".format(config.datapath, month_str))
                time.sleep(5)
                handler.tick(5)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

    else:
        rmob_export(config, args.upload, args.denoise, args.month)


class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, handler_func):
        self.file_path = None
        self.cooldown = 0
        self._handler_func = handler_func

    def on_any_event(self, event):
        if self.cooldown > 0:
            return
        if self.file_path != event.src_path:
            return
        if event.event_type not in ("created", "modified"):
            return
        self.cooldown += 1500
        self._handler_func()

    def set_file_path(self, file_path):
        self.file_path = file_path

    def set_handler_func(self, handler_func):
        self._handler_func = handler_func

    def tick(self, seconds):
        self.cooldown = max(self.cooldown - seconds, 0)


def rmob_export(config, upload, denoise, month):
    if month is None:
        _month = datetime.datetime.now()
    else:
        _month = datetime.datetime.strptime(month, "%Y-%m")
    data = RmobData(config, _month)
    path_txt = data.export_rmob_txt()
    img = RmobColorgramme(data)
    img.render()
    path_img = img.save()
    if upload and config.upload_to_rmob:
        session = ftplib.FTP("217.169.242.217", "radiodata", "meteor")
        for path in (path_txt, path_img):
            filename = path.split("/")[-1]
            with open(path, "rb") as stream:
                session.storbinary("STOR /{}".format(filename), stream)
        session.quit()


def dms2dec(dms_str):
    """
    Return decimal representation of DMS
    https://gist.github.com/jeteon/89c41e4081d87b798d8006b16a52c695
    """
    dms_str = re.sub(r"\s", "", dms_str)
    sign = -1 if re.search("[swSW]", dms_str) else 1
    numbers = list(filter(len, re.split("\D+", dms_str, maxsplit=4)))
    degree = numbers[0]
    minute = numbers[1] if len(numbers) >= 2 else "0"
    second = numbers[2] if len(numbers) >= 3 else "0"
    frac_seconds = numbers[3] if len(numbers) >= 4 else "0"
    second += "." + frac_seconds
    return sign * (int(degree) + float(minute) / 60 + float(second) / 3600)


class RmobConfig:
    INFO_FIELDS = (
        "observer",
        "country",
        "city",
        "location",
        "beacon",
        "frequency",
        "antenna",
        "computer",
        "receiver",
        "preamp",
        "azimuth",
        "elevation",
        "method",
        "website",
        "email",
    )

    def __init__(self, path="rmob.yaml"):
        with open(path, "r") as stream:
            self.__dict__.update(yaml.load(stream))
        self.version = "msas-rmobgen v1.0 (https://github.com/p-sherratt/msas-rmobgen)"
        for f in __class__.INFO_FIELDS:
            if f not in self.info:
                self.info[f] = ""
            if self.info[f] is None:
                self.info[f] = ""

        loc = self.info["location"].split(" ")
        if loc[0][-2].isdigit():
            loc[0] = loc[0][:-1] + " " + loc[0][-1:]
        if loc[1][-2].isdigit():
            loc[1] = loc[1][:-1] + " " + loc[1][-1:]

        lng_then_lat = loc[0][-1] in ("N", "S")
        self.lat = loc[not lng_then_lat]
        self.lng = loc[lng_then_lat]
        self.lat_dec = dms2dec(loc[not lng_then_lat])
        self.lng_dec = dms2dec(loc[lng_then_lat])


class RmobData:
    def __init__(self, rmob_config, month=None):
        self.config = rmob_config
        self.diurnal = {}
        self.first_date = 3652059
        self.last_date = 0
        self.thresholds = {}
        self.update(month)

    def update(self, month=None):
        if month is None:
            month = datetime.datetime.today()
        month_str = month.strftime("%Y%m")
        path = "{}/RMOB-{}.dat".format(self.config.datapath, month_str)
        date_ord = month.toordinal()

        rows = []
        with open(path, "r") as data:
            rows = data.read().replace(" ", "").split("\n")

        diurnal = defaultdict(dict)

        for row in rows:
            cols = row.split(",")

            if len(cols) < 3:
                continue

            year = cols[0][:4]
            month = cols[0][4:6]
            day = cols[0][6:8]
            hour = cols[1]
            count = int(cols[2])

            date = datetime.date(year=int(year), month=int(month), day=int(day))
            date_ord = date.toordinal()
            date_str = "{}-{}-{}".format(year, month, day)
            diurnal[date_str][int(hour)] = count

            self.first_date = min(self.first_date, date_ord)
            self.last_date = max(self.last_date, date_ord)

        counts = []
        for date in sorted(diurnal):
            counts.extend(diurnal[date].values())
        c = sum(counts) / len(counts)
        sd = (sum((x - c) ** 2 for x in counts) / (len(counts) + 1)) ** 0.5
        max_count = min(c + sd * 1.28, max(counts))

        self.diurnal.update(diurnal)
        self.thresholds.update({date_ord: max_count})

    def export_rmob_txt(self, path=None):
        date = datetime.date.fromordinal(self.last_date)
        info = self.config.info
        _, days_in_month = monthrange(date.year, date.month)
        if path is None:
            month_rev = date.strftime("%m%Y")
            path = "{}_{}rmob.TXT".format(self.config.outfile_prefix, month_rev)

        with open(path, "w") as stream:
            heading = date.strftime("%b").lower() + "|"
            heading += "".join(" {:02}h|".format(h) for h in range(0, 24))
            stream.write(heading + "\n")
            for day in range(1, days_in_month + 1):
                date = date.replace(day=day)
                date_str = date.strftime("%Y-%m-%d")

                try:
                    counts = self.diurnal[date_str]
                except KeyError:
                    counts = {}

                try:
                    threshold = self.thresholds[date.toordinal()]
                except KeyError:
                    threshold = 999999

                row = " {:02}|".format(day)
                for h in range(24):
                    if h not in counts or counts[h] > threshold:
                        row += "??? |"
                        continue
                    count = "{:03}".format(counts[h])
                    count = "{:4}|".format(count)
                    row += count
                stream.write(row + "\n")

            for field in (
                ('Observer', info['observer']),
                ('Country', info['country']),
                ('City', info['city']),
                ('Longitude', self.config.lng),
                ('Latitude ', self.config.lat),
                ('Longitude GMAP', self.config.lng_dec),
                ('Frequencies', self.config.lat_dec),
                ('Antenna', info['antenna']),
                ('Pre-Amplifier', info['preamp']),
                ('Receiver', info['receiver']),
                ('Observing Method', info['method']),
                ('Remarks', info['computer']),
                ('Soft FTP', self.config.version),
                ('E', info['email'])
            ):
                stream.write("[{}]{}\n".format(*field))

        return path


class RmobColorgramme:
    LABEL_MAP = {"method": "Obs.Method", "preamp": "RF preamp."}
    VALUE_MAP = {
        "location": lambda x: x.replace(" ", "\n")
        .replace("W", " West")
        .replace("E", " East")
        .replace("N", " North")
        .replace("S", " South"),
        "azimuth": lambda x: str(x) + "°",
        "elevation": lambda x: str(x) + "°",
    }

    IMG_FONT = ImageFont.truetype("resources/ubuntu.ttf", 11)
    IMG_FONT_SM = ImageFont.truetype("resources/ubuntu.ttf", 10)

    def __init__(self, rmob_data):
        self.data = rmob_data

    def render(self, start_date=None, end_date=None, plot_type="month"):
        if plot_type == "month":
            self.render_month(start_date)
        else:
            raise Exception("unsupported plot type: {}".format(plot_type))

    def render_month(self, month=None):
        if month is None:
            month = datetime.date.fromordinal(self.data.last_date)

        self._img = Image.new("RGB", (700, 220), color=(255, 255, 255))
        self._img_draw = ImageDraw.Draw(self._img)
        bottom = self._render_info()
        if bottom < 112:
            self._img_draw.text(
                (15, 120),
                "Hourly count\nhistogram",
                font=__class__.IMG_FONT,
                fill="black",
            )
        self._render_logo()
        self._render_histogram()
        self._render_heatmap()
        self._render_website()

    def _render_logo(self, xy=(10, 164)):
        try:
            logo = Image.open(self.data.config.info["logo"], "r")
            self._img.paste(logo, xy)
        except:
            pass

    def _render_info(self, xy=(3, 1)):
        info = self.data.config.info
        d = self._img_draw

        def draw_info_column(x, y, labels, value_offset=57):
            for label in labels:
                if label not in info or not info[label]:
                    continue
                try:
                    _label = __class__.LABEL_MAP[label]
                except KeyError:
                    _label = label

                try:
                    _value = __class__.VALUE_MAP[label](info[label])
                except KeyError:
                    _value = info[label]

                _label = _label[0].upper() + _label[1:] + ":"
                d.text((x, y), _label, font=__class__.IMG_FONT, fill=(64, 64, 64))
                for line in str(_value).split("\n"):
                    d.text(
                        (x + value_offset, y),
                        line,
                        font=__class__.IMG_FONT,
                        fill=(0, 0, 128),
                    )
                    y += 15

            return y

        y = draw_info_column(xy[0], xy[1], ["observer", "country", "city", "computer"])
        y = draw_info_column(xy[0], y, ["antenna", "preamp", "azimuth", "elevation"])
        y = draw_info_column(xy[0], y, ["email"])
        bottom = y
        y = draw_info_column(
            xy[0] + 200, xy[1], ["location", "beacon", "frequency", "receiver"]
        )
        y = draw_info_column(xy[0] + 200, y, ["method"], 75)
        return bottom

    def _render_website(self, xy=(412, 207)):
        d = self._img_draw

        try:
            website = self.data.config.info["website"] + "  |  "
        except KeyError:
            website = ""
        except TypeError:
            website = ""

        website_width = d.textsize(website, font=__class__.IMG_FONT)[0]
        rmob_width = d.textsize("www.rmob.org", font=__class__.IMG_FONT)[0]
        x = xy[0] + int((250 - rmob_width - website_width) / 2)
        d.text((x, xy[1]), website, font=__class__.IMG_FONT, fill=(64, 64, 64))
        d.text(
            (x + website_width, xy[1]),
            "www.rmob.org",
            font=__class__.IMG_FONT,
            fill=(128, 64, 64),
        )

    def _render_heatmap(self, xy=(407, 15)):
        d = self._img_draw
        peak = (0, 0)
        threshold_avg = sum(self.data.thresholds.values()) / len(self.data.thresholds)

        for date in self.data.diurnal:
            hours = self.data.diurnal[date]
            date_ord = datetime.datetime.strptime(date, "%Y-%m-%d").toordinal()
            try:
                threshold = self.data.thresholds[date_ord]
            except KeyError:
                threshold = threshold_avg

            for hour, count in hours.items():
                if count > peak[1] and count <= threshold:
                    peak = (hour, count)

        # draw canvas for heatmap & scale bar
        d.rectangle([xy, (xy[0] + 247, xy[1] + 191)], outline="black", fill="black")
        d.rectangle(
            [(xy[0] + 250, xy[1]), (xy[0] + 257, xy[1] + 191)],
            outline="black",
            fill="black",
        )

        # draw y-axis tick markers
        for hour in range(24):
            y = xy[1] + hour * 8 - 2
            if hour % 6 == 0 or hour == 23:
                text_size = d.textsize(str(hour) + "h", font=__class__.IMG_FONT)
                d.text(
                    (xy[0] - text_size[0] - 4, y),
                    str(hour) + "h",
                    font=__class__.IMG_FONT,
                    fill="black",
                )
            elif hour % 3 == 0:  # major tick markers
                d.line([(xy[0] - 5, y + 6), (xy[0] - 1, y + 6)], "black")
            else:  # minor
                d.line([(xy[0] - 2, y + 6), (xy[0] - 1, y + 6)], "black")

        # draw x-axis tick markers
        d.text(
            (xy[0] - 32, xy[1] + 20), "UTC", font=__class__.IMG_FONT, fill=(32, 32, 32)
        )
        d.text((xy[0] + 1, xy[1] - 14), "1", font=__class__.IMG_FONT, fill="black")
        d.text(
            (xy[0] + 15, xy[1] - 14), "Days --->", font=__class__.IMG_FONT, fill="black"
        )
        d.text((xy[0] + 111, xy[1] - 14), "15", font=__class__.IMG_FONT, fill="black")
        d.text((xy[0] + 239, xy[1] - 14), "31", font=__class__.IMG_FONT, fill="black")

        # draw scale bar
        for hour in range(24):
            d.rectangle(
                [(xy[0] + 251, 16 + hour * 8), (xy[0] + 256, 21 + hour * 8)],
                fill=self._get_color(hour, 24),
            )

        d.text((xy[0] + 260, xy[1] - 1), "0", font=__class__.IMG_FONT, fill="black")
        d.text(
            (xy[0] + 260, xy[1] + 91),
            str(int(peak[1] / 2)),
            font=__class__.IMG_FONT,
            fill="black",
        )
        d.text(
            (xy[0] + 260, xy[1] + 182),
            str(int(peak[1])),
            font=__class__.IMG_FONT,
            fill="black",
        )

        # draw heatmap squares
        for date in sorted(self.data.diurnal):
            hours = self.data.diurnal[date]
            day = int(date[8:])
            for hour in sorted(hours):
                count = hours[hour]
                color = self._get_color(count, max(1, peak[1]))
                x = xy[0] + day * 8 - 7
                y = 16 + hour * 8
                d.rectangle([(x, y), (x + 5, y + 5)], fill=color)

    def _render_histogram(self, xy=(120, 110)):
        d = self._img_draw
        date = datetime.date.fromordinal(self.data.last_date)

        # draw outline
        d.rectangle([xy, (xy[0] + 245, xy[1] + 95)], outline="black")

        # draw date as title
        day_suffix = (
            "th"
            if 4 <= date.day <= 20 or 24 <= date.day <= 30
            else ("st", "nd", "rd")[date.day % 10 - 1]
        )
        title = date.strftime("%B %-d{S} %Y").replace("{S}", day_suffix)
        title_size = d.textsize(title, font=__class__.IMG_FONT)
        title_x = int(xy[0] + 122.5 - title_size[0] / 2)
        title_y = int(xy[1] - title_size[1] / 2)
        d.rectangle(
            [
                (title_x - 3, title_y - 3),
                (title_x + title_size[0] + 3, title_y + title_size[1] + 3),
            ],
            fill="white",
        )
        d.text((title_x, title_y), title, font=__class__.IMG_FONT, fill=(0, 96, 0))

        # determine peak for histogram bars
        threshold = self.data.thresholds[self.data.last_date]
        date_str = date.strftime("%Y-%m-%d")
        try:
            counts = self.data.diurnal[date_str]
        except:
            counts = {}
        peak = (0, 0)
        for hour, count in counts.items():
            if count > peak[1] and count <= threshold:
                peak = (hour, count)

        # draw histogram bars, x-axis ticks and labels
        for hour in range(25):
            x = xy[0] + 5 + hour * 10

            # tick/marker
            d.line([(x, xy[1] + 94), (x, xy[1] + 97)], "black")
            if hour % 4 == 0:
                d.text(
                    (x - 4, xy[1] + 98),
                    str(hour) + "h",
                    font=__class__.IMG_FONT,
                    fill="black",
                )

            if hour not in counts:
                continue

            if counts[hour] > threshold:
                d.rectangle(
                    [(x - 3, xy[1] + 6), (x + 3, xy[1] + 95)],
                    fill=(255, 192, 192),
                    outline=(128, 128, 128),
                )
            else:
                bar_height = 88 * counts[hour] / max(peak[1], 1)
                d.rectangle(
                    [(x - 3, xy[1] + 95 - bar_height), (x + 3, xy[1] + 95)],
                    outline="black",
                    fill="blue",
                )
                if hour == peak[0]:
                    # peak y-axis label & dashed line to peak bar
                    x1 = xy[0] - 3
                    while x1 < x - 5:
                        d.line(
                            [
                                (x1, xy[1] + 95 - bar_height),
                                (x1 + 2, xy[1] + 95 - bar_height),
                            ],
                            "black",
                        )
                        x1 += 6
                    text_size = d.textsize(str(peak[1]), font=__class__.IMG_FONT)
                    d.text(
                        (xy[0] - 5 - text_size[0], xy[1] + 90 - bar_height),
                        str(peak[1]),
                        font=__class__.IMG_FONT,
                        fill="black",
                    )

    def _get_color(self, value, max_value):
        value = float(value)
        max_value = float(max_value)
        scale_255 = float(255) / max_value
        domains = [
            (value < 0, (5, 5, 5)),
            (value <= max_value / 3, (0, int(3 * value * scale_255), 255)),
            (
                value <= max_value / 3 * 2,
                (
                    int(3 * value * scale_255 - 255),
                    255,
                    int(3 * (max_value - value) * scale_255 - 255),
                ),
            ),
            (value <= max_value, (255, int(3 * (max_value - value) * scale_255), 0)),
            (value > max_value, (128, 128, 128)),
        ]
        for condition, rgb in domains:
            if condition:
                return rgb

    def save(self, path=None):
        if not hasattr(self, "_img"):
            raise Exception("colorgramme first needs to be .render()'ed before saving")

        if path is None:
            month_rev = datetime.date.fromordinal(self.data.last_date).strftime("%m%Y")
            path = "{}_{}.jpg".format(self.data.config.outfile_prefix, month_rev)

        kwargs = {}
        if path.endswith(".jpg"):
            kwargs["quality"] = 90

        self._img.save(path, **kwargs)
        return path


class NegateAction(argparse.Action):
    def __call__(self, parser, ns, values, option):
        setattr(ns, self.dest, option[2:4] != "no")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate and upload RMOB colorgrammes & data."
    )
    parser.add_argument(
        "--upload", action="store_true", help="upload generated files to rmob"
    )
    parser.add_argument(
        "--denoise",
        "--no-denoise",
        dest="denoise",
        action=NegateAction,
        help="detect and mask outliers",
    )
    parser.add_argument("--month", help="month to generate data for (YYYY-MM)")
    parser.add_argument(
        "--watch",
        "-w",
        action="store_true",
        help="run forever, watching for data file updates",
    )
    parser.add_argument("config_file", type=str, help="path to rmob config file")
    args = parser.parse_args()
    rmob_main(args)


