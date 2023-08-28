import argparse
import datetime
import gzip
import io
import logging
import re
import requests
import time
from typing import Union
import xml.etree.ElementTree as ET

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import numpy as np
import pandas as pd

class CreateMjlog(beam.DoFn):
    def process(self, timestamp) -> list[dict[str, Union[str, datetime.datetime]]]:
        headers = {'User-Agent': 'Mozilla/5.0'}
        fileindex_url = "https://tenhou.net/sc/raw/list.cgi"
        response = requests.get(fileindex_url, headers=headers)
        fileindex = response.text
        fileindex_scc_df = self.convert_log_text_to_eval(fileindex, date_now=timestamp)
        mjlog_df = self.create_mjlog_df(fileindex_scc_df, date_now=timestamp)
        mjlog_dict = mjlog_df.to_dict(orient="records")
        return mjlog_dict

    def convert_log_text_to_eval(self, log_text:str, date_now:datetime.datetime) -> pd.DataFrame:
        log_text_preformatted = log_text.replace("\r\n", "").replace("\r\n", "").replace("\r\n", "").replace(
            "\r\n", "").replace(r'file', r'"file"').replace('size', '"size"').rstrip(";")
        log_list = eval(log_text_preformatted)
        fileindex_df = pd.DataFrame(log_list)
        date_yesterday = date_now - datetime.timedelta(days=1)
        date_yesterday_str = date_yesterday.replace(tzinfo=None).strftime('%Y%m%d')
        fileindex_scc_yesterday_df = fileindex_df[fileindex_df.file.str.match(f"scc{date_yesterday_str}.+?html.gz")]
        return fileindex_scc_yesterday_df

    def create_mjlog_df(self, fileindex_scc_df:pd.DataFrame, date_now:datetime.datetime) -> pd.DataFrame:
        headers = {'User-Agent': 'Mozilla/5.0'}
        url_log_root = "https://tenhou.net/sc/raw/dat/"
        data_dict_list = []
        for file in fileindex_scc_df.file:
            url_log = url_log_root + file
            response = requests.get(url_log, headers=headers)
            gzip_file = io.BytesIO(response.content)
            with gzip.open(gzip_file, 'rt') as f:
                json_data = f.read()
                mjlog_list = json_data.split("\n")
                for mjlog in mjlog_list:
                    if len(mjlog) > 5:
                        mjlog_dict = self.create_mjlog_dict(mjlog, date_now=date_now)
                        data_dict_list.append(mjlog_dict)
        mjlog_df = pd.DataFrame(data_dict_list)
        mjlog_df_use = mjlog_df.query('room_type=="四鳳南喰赤－" | room_type=="四鳳東喰赤－"').copy()
        mjlog_df_use["game_unit"] = mjlog_df_use.room_type.apply(lambda x: x[2])
        mjlog_df_use["mjlog"] = mjlog_df_use.mjlog_id.apply(lambda x: self.search_mjlog(x))
        return mjlog_df_use

    def create_mjlog_dict(self, mjlog:str, date_now:datetime.datetime) -> dict[str, Union[datetime.datetime, str]]:
        time_str, _, room_type_str, mjlog_url_str, player_str = mjlog.split(" | ")
        gamestart_time = datetime.datetime.strptime(time_str, '%H:%M')
        date_yesterday = date_now - datetime.timedelta(days=1)
        created_date = date_now.replace(tzinfo=None)
        gamestart_date = date_yesterday.replace(tzinfo=None).replace(hour=gamestart_time.hour, minute=gamestart_time.minute, second=0, microsecond=0)
        mjlog_id = re.findall('log=(.*)"', mjlog_url_str)[0]
        player_list = player_str.rstrip("<br>")
        mjlog_dict = {"created_date":created_date, "gamestart_date":gamestart_date, "room_type": room_type_str,
                            "mjlog_id": mjlog_id, "player_list": player_list}
        return mjlog_dict

    def search_mjlog(self, mjlog_id:str) -> str:
        headers = {'User-Agent': 'Mozilla/5.0'}
        moto_url = r"https://tenhou.net/0/log/?"
        mjlog_url = moto_url + mjlog_id
        response = requests.get(mjlog_url, headers=headers)
        mjlog = response.text
        return mjlog

class GameData():
    def __init__(self):
        self.kyoku_num_array = np.array([])
        self.agari_rate_array = np.array([])
        self.hoju_rate_array = np.array([])
        self.dama_rate_array = np.array([])
        self.reach_rate_array = np.array([])
        self.furo_rate_array = np.array([])
        self.furo_num_array = np.array([])
        self.furo_num_per_kyoku_array = np.array([])
        self.tsumo_rate_array = np.array([])
        self.agari_point_array = np.array([])
        self.agari_turn_array = np.array([])
        self.ryukyoku_rate_array = np.array([])
        self.ryukyoku_tenpai_rate_array = np.array([])

    def set_game_data(self):
        self.kyoku_num = 0
        self.agari_game_array = np.array([0, 0, 0, 0])
        self.hoju_game_array = np.array([0, 0, 0, 0])
        self.dama_game_array = np.array([0, 0, 0, 0])
        self.reach_game_array = np.array([0, 0, 0, 0])
        self.furo_boolen_game_array = np.array([0, 0, 0, 0])
        self.furo_num_game_array = np.array([0, 0, 0, 0])
        self.tsumo_game_array = np.array([0, 0, 0, 0])
        self.agari_point_game_array = np.array([0, 0, 0, 0])
        self.agari_turn_num = 0
        self.ryukyoku_num = 0
        self.ryukyoku_tenpai_game_array = np.array([0, 0, 0, 0])

    def set_kyoku_data(self):
        self.agari_kyoku_array = np.array([0, 0, 0, 0])
        self.hoju_kyoku_array = np.array([0, 0, 0, 0])
        self.dama_kyoku_array = np.array([0, 0, 0, 0])
        self.reach_kyoku_array = np.array([0, 0, 0, 0])
        self.furo_boolen_kyoku_array = np.array([0, 0, 0, 0])
        self.furo_num_kyoku_array = np.array([0, 0, 0, 0])
        self.tsumo_kyoku_array = np.array([0, 0, 0, 0])
        self.agari_point_kyoku_array = np.array([0, 0, 0, 0])
        self.ryukyoku_tenpai_kyoku_array = np.array([0, 0, 0, 0])

    def cumsum_kyoku_data(self):
        self.agari_game_array += self.agari_kyoku_array
        self.hoju_game_array += self.hoju_kyoku_array
        self.dama_game_array += self.dama_kyoku_array
        self.reach_game_array += self.reach_kyoku_array
        self.furo_boolen_game_array += self.furo_boolen_kyoku_array
        self.furo_num_game_array += self.furo_num_kyoku_array
        self.tsumo_game_array += self.tsumo_kyoku_array
        self.agari_point_game_array += self.agari_point_kyoku_array
        self.ryukyoku_tenpai_game_array += self.ryukyoku_tenpai_kyoku_array

    def aggregate_data(self):
        self.kyoku_num_array = np.hstack([self.kyoku_num_array, np.array(self.kyoku_num)])
        self.agari_rate_array = np.hstack([self.agari_rate_array, self.agari_game_array / self.kyoku_num])
        self.hoju_rate_array = np.hstack([self.hoju_rate_array, self.hoju_game_array / self.kyoku_num])
        self.dama_rate_array = np.hstack([self.dama_rate_array, self.dama_game_array / self.kyoku_num])
        self.reach_rate_array = np.hstack([self.reach_rate_array, self.reach_game_array / self.kyoku_num])
        self.furo_rate_array = np.hstack([self.furo_rate_array, self.furo_boolen_game_array / self.kyoku_num])
        self.furo_num_array = np.hstack([self.furo_num_array, self.furo_num_game_array])
        self.furo_num_per_kyoku_array = np.hstack([self.furo_num_per_kyoku_array, self.furo_num_game_array / self.kyoku_num])
        self.tsumo_rate_array = np.hstack([self.tsumo_rate_array, self.tsumo_game_array / self.kyoku_num])
        self.agari_point_array = np.hstack([self.agari_point_array, self.agari_point_game_array])
        self.agari_turn_array = np.hstack([self.agari_turn_array, self.agari_turn_num])
        self.ryukyoku_rate_array = np.hstack([self.ryukyoku_rate_array, self.ryukyoku_num / self.kyoku_num])
        self.ryukyoku_tenpai_rate_array = np.hstack([self.ryukyoku_tenpai_rate_array,
                                                    self.ryukyoku_tenpai_game_array / self.kyoku_num])

    def calc_game_data(self, mjlog:str):
        # ゲーム開始
        root = ET.fromstring(mjlog)
        self.set_game_data()
        self.set_kyoku_data()
        for child in root:
            # 局開始
            if child.tag=="INIT":
                self.kyoku_num += 1
                self.cumsum_kyoku_data()
                self.set_kyoku_data()
            elif child.tag=="N":
                self.furo_boolen_kyoku_array[int(child.attrib["who"])] = 1
                self.furo_num_kyoku_array[int(child.attrib["who"])] += 1
            elif child.tag=="REACH":
                if child.attrib["step"]=="2":
                    self.reach_kyoku_array[int(child.attrib["who"])] = 1
            elif child.tag=="AGARI":
                self.agari_kyoku_array[int(child.attrib["who"])] = 1
                self.agari_point_kyoku_array[int(child.attrib["who"])] = int(child.attrib["ten"].split(",")[1])
                if child.attrib["who"]==child.attrib["fromWho"]:
                    self.tsumo_kyoku_array[int(child.attrib["who"])] = 1
                else:
                    self.hoju_kyoku_array[int(child.attrib["fromWho"])] = 1
                if "owari" in child.attrib:
                    break
            elif child.tag=="RYUUKYOKU":
                self.ryukyoku_num += 1
                if "owari" in child.attrib:
                    break
        if self.kyoku_num!=0:
            self.cumsum_kyoku_data()
            self.aggregate_data()

class CalcStats(beam.DoFn):
    def process(self, mjlog_dict) -> list[dict[str, Union[str, float, datetime.datetime]]]:
        game_data = GameData()
        game_data.calc_game_data(mjlog_dict["mjlog"])
        gamestart_date = mjlog_dict["gamestart_date"]
        game_unit = mjlog_dict["game_unit"]
        game_data_array = np.array([
            np.tile(game_data.kyoku_num_array, 4),
            game_data.agari_rate_array,
            game_data.tsumo_rate_array,
            game_data.reach_rate_array,
            game_data.furo_rate_array,
            game_data.furo_num_array,
            game_data.furo_num_per_kyoku_array,
            game_data.agari_point_array,
            np.tile(game_data.ryukyoku_rate_array, 4)
            ]).T
        output = []
        for game_data_person in game_data_array:
            stats_dict = {"gamestart_date": gamestart_date,
                            "game_unit": game_unit,
                            "kyoku_num": game_data_person[0],
                            "agari_rate": game_data_person[1],
                            "tsumo_rate": game_data_person[2],
                            "reach_rate": game_data_person[3],
                            "furo_rate": game_data_person[4],
                            "furo_num": game_data_person[5],
                            "furo_num_per_kyoku": game_data_person[6],
                            "agari_point": game_data_person[7],
                            "ryukyoku_rate": game_data_person[8]}
            output.append(stats_dict)
        return output

class AddTimestamp(beam.DoFn):
    def process(self, element) -> list[datetime.datetime]:
        timestamp = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=+9), 'JST'))
        yield timestamp

def run(argv=None):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(flags=pipeline_args)

    table_name_mjlog = "tenhou.mjlog"
    table_name_stats = "tenhou.stats"
    table_schema_mjlog = "mjlog_id:STRING,room_type:STRING,game_unit:STRING,mjlog:STRING,created_date:DATETIME,gamestart_date:DATETIME,player_list:STRING"
    table_schema_stats = "gamestart_date:DATETIME,game_unit:STRING,kyoku_num:FLOAT,agari_rate:FLOAT,tsumo_rate:FLOAT,reach_rate:FLOAT,furo_rate:FLOAT,furo_num:FLOAT,furo_num_per_kyoku:FLOAT,agari_point:FLOAT,ryukyoku_rate:FLOAT"

    PROJECTID = "poc-api-test"
    query = """
            SELECT gamestart_date,game_unit,mjlog
            FROM tenhou.mjlog
            ;
    """

    with beam.Pipeline(options=options) as p:
        mjlog_pcollection = (p
               | 'CreatePseudoCol' >> beam.Create(["element"])
               | 'AddTimestamp' >> beam.ParDo(AddTimestamp())
               | 'CreateMjlogCol' >> beam.ParDo(CreateMjlog())
            )
        output_to_datalake = (mjlog_pcollection
            | 'WriteToDatalake' >> beam.io.WriteToBigQuery('{}:{}'.format(
                PROJECTID, table_name_mjlog), schema=table_schema_mjlog)
            )
        output_to_datamart = (mjlog_pcollection
            | 'ConvertStats' >> beam.ParDo(CalcStats())
            | 'WriteToDatamart' >> beam.io.WriteToBigQuery('{}:{}'.format(
                PROJECTID, table_name_stats), schema=table_schema_stats)
            )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()