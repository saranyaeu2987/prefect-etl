from prefect import Flow, Parameter
from prefect.engine.executors import DaskExecutor
from prefect.schedules import CronSchedule
from datetime import datetime as dt, timedelta
from prefect.engine.results import LocalResult

from . import Constants
from .task import (
    write_delta_lookup_table,
    final_task,
    get_S3_connection
)

# hourly schedule
#hourly_schedule = CronSchedule("0 * * * *")

with Flow(f"test-etl") as flow:
    start = Parameter("start")
    #s3_connection = get_S3_connection()
    s3_bucket = Constants.S3_SOURCE_BUCKET
    delta_raw_prefix = Constants.DELTA_RAW_PREFIX
    previous_dt = dt.today() - timedelta(hours=1)
    #date_hour_path = f"{previous_dt.strftime('%Y-%m-%d')}/{previous_dt.strftime('%H')}"
    date_hour_path = Parameter("date_hour_path",default="2020-09-01/18")

    company = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                       s3_source_bucket_raw_prefix=Constants.S3_COMPANY,
                                       dest_delta_prefix=delta_raw_prefix, table_name="test.company",
                                       date_hour_path=date_hour_path, pk=Constants.PK_COMPANY)
    lin_of_bus = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                          s3_source_bucket_raw_prefix=Constants.S3_LIN_OF_BUS,
                                          dest_delta_prefix=delta_raw_prefix, table_name="test.lin_of_bus",
                                          date_hour_path=date_hour_path, pk=Constants.PK_LIN_OF_BUS)
    ck_coll_cntl = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                            s3_source_bucket_raw_prefix=Constants.S3_CK_COLL_CNTL_PREFIX,
                                            dest_delta_prefix=delta_raw_prefix, table_name="test.ck_coll_cntl",
                                            date_hour_path=date_hour_path, pk=Constants.PK_CK_COLL_CNTL)
    cmptr_typ = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                         s3_source_bucket_raw_prefix=Constants.S3_CMPTR_TYP,
                                         dest_delta_prefix=delta_raw_prefix, table_name="test.cmptr_typ",
                                         date_hour_path=date_hour_path, pk=Constants.PK_CMPTR_TYP)
    dist = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                    s3_source_bucket_raw_prefix=Constants.S3_DIST,
                                    dest_delta_prefix=delta_raw_prefix, table_name="test.dist",
                                    date_hour_path=date_hour_path, pk=Constants.PK_DIST)
    ecomm_mkt_area = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                              s3_source_bucket_raw_prefix=Constants.S3_ECOMM_MKT_AREA,
                                              dest_delta_prefix=delta_raw_prefix, table_name="test.ecomm_mkt_area",
                                              date_hour_path=date_hour_path, pk=Constants.PK_ECOMM_MKT_AREA)
    financial_div = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                             s3_source_bucket_raw_prefix=Constants.S3_FINANCIAL_DIV,
                                             dest_delta_prefix=delta_raw_prefix, table_name="test.financial_div",
                                             date_hour_path=date_hour_path, pk=Constants.PK_FINANCIAL_DIV)
    frnt_end = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                        s3_source_bucket_raw_prefix=Constants.S3_FRNT_END,
                                        dest_delta_prefix=delta_raw_prefix, table_name="test.frnt_end",
                                        date_hour_path=date_hour_path, pk=Constants.PK_FRNT_END)
    location_typ = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                            s3_source_bucket_raw_prefix=Constants.S3_LOCATION_TYP,
                                            dest_delta_prefix=delta_raw_prefix, table_name="test.location_typ",
                                            date_hour_path=date_hour_path, pk=Constants.PK_LOCATION_TYPE)
    mkt_rgn = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                       s3_source_bucket_raw_prefix=Constants.S3_MKT_RGN,
                                       dest_delta_prefix=delta_raw_prefix, table_name="test.mkt_rgn",
                                       date_hour_path=date_hour_path, pk=Constants.PK_MKT_RGN)
    retl_loc_frmt = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                             s3_source_bucket_raw_prefix=Constants.S3_RETL_LOC_FRMT,
                                             dest_delta_prefix=delta_raw_prefix, table_name="test.retl_loc_frmt",
                                             date_hour_path=date_hour_path, pk=Constants.PK_RETL_LOC_FRMAT)
    retl_loc_segm = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                             s3_source_bucket_raw_prefix=Constants.S3_RETL_LOC_SEGM,
                                             dest_delta_prefix=delta_raw_prefix, table_name="test.retl_loc_segm",
                                             date_hour_path=date_hour_path, pk=Constants.PK_RETL_LOC_SEGM)
    retl_loc_stat_typ = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                                 s3_source_bucket_raw_prefix=Constants.S3_RETL_LOC_STAT_TYP,
                                                 dest_delta_prefix=delta_raw_prefix, table_name="test.retl_loc_stat_typ",
                                                 date_hour_path=date_hour_path, pk=Constants.PK_RETL_LOC_STAT_TYP)
    retl_loc_typ = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                            s3_source_bucket_raw_prefix=Constants.S3_RETL_LOC_TYP,
                                            dest_delta_prefix=delta_raw_prefix, table_name="test.retl_loc_typ",
                                            date_hour_path=date_hour_path, pk=Constants.PK_RETL_LOC_TYP)
    retl_unld = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                         s3_source_bucket_raw_prefix=Constants.S3_RETL_UNLD,
                                         dest_delta_prefix=delta_raw_prefix, table_name="test.retl_unld",
                                         date_hour_path=date_hour_path, pk=Constants.PK_RETL_UNLD)
    srs_sys = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                       s3_source_bucket_raw_prefix=Constants.S3_SRS_SYS,
                                       dest_delta_prefix=delta_raw_prefix, table_name="test.srs_sys",
                                       date_hour_path=date_hour_path, pk=Constants.PK_SRS_SYS)
    str_opstmt_hdr_typ = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                                  s3_source_bucket_raw_prefix=Constants.S3_STR_OPSTMT_HDR_TYP,
                                                  dest_delta_prefix=delta_raw_prefix,
                                                  table_name="test.str_opstmt_hdr_typ", date_hour_path=date_hour_path,
                                                  pk=Constants.PK_STR_OPSTMT_HDR_TYP)
    str_opstmt_typ = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                              s3_source_bucket_raw_prefix=Constants.S3_STR_OPSTMT_TYP,
                                              dest_delta_prefix=delta_raw_prefix, table_name="test.str_opstmt_typ",
                                              date_hour_path=date_hour_path, pk=Constants.PK_STR_OPSTMT_TYP)

    retail_loc = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                          s3_source_bucket_raw_prefix=Constants.S3_RETAIL_LOC,
                                          dest_delta_prefix=delta_raw_prefix, table_name="test.retail_loc",
                                          date_hour_path=date_hour_path, pk=Constants.PK_RETAIL_LOC)
    location = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                        s3_source_bucket_raw_prefix=Constants.S3_LOCATION,
                                        dest_delta_prefix=delta_raw_prefix, table_name="test.location",
                                        date_hour_path=date_hour_path, pk=Constants.PK_LOCATION)
    heb_dt = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                      s3_source_bucket_raw_prefix=Constants.S3_HEB_DT,
                                      dest_delta_prefix=delta_raw_prefix, table_name="test.heb_dt",
                                      date_hour_path=date_hour_path, pk=Constants.PK_HEB_DT)
    str_opstmt_srt_cd = write_delta_lookup_table( s3_source_bucket=s3_bucket,
                                                 s3_source_bucket_raw_prefix=Constants.S3_STR_OPSTMT_SRT_CD,
                                                 dest_delta_prefix=delta_raw_prefix, table_name="test.str_opstmt_srt_cd",
                                                 date_hour_path=date_hour_path, pk=Constants.PK_STR_OPSTMT_SRT_CD)

    #start.set_downstream( flow=flow)
    start.set_downstream(lin_of_bus, flow=flow)
    start.set_downstream(company, flow=flow)
    start.set_downstream(ck_coll_cntl, flow=flow)
    start.set_downstream(cmptr_typ, flow=flow)
    start.set_downstream(dist, flow=flow)
    start.set_downstream(ecomm_mkt_area, flow=flow)
    start.set_downstream(financial_div, flow=flow)
    start.set_downstream(frnt_end, flow=flow)
    start.set_downstream(location_typ, flow=flow)
    start.set_downstream(mkt_rgn, flow=flow)
    start.set_downstream(retl_loc_frmt, flow=flow)
    start.set_downstream(retl_loc_segm, flow=flow)
    start.set_downstream(retl_loc_stat_typ, flow=flow)
    start.set_downstream(retl_loc_typ, flow=flow)
    start.set_downstream(retl_unld, flow=flow)
    start.set_downstream(srs_sys, flow=flow)
    start.set_downstream(str_opstmt_hdr_typ, flow=flow)
    start.set_downstream(str_opstmt_typ, flow=flow)
    start.set_downstream(retail_loc, flow=flow)
    start.set_downstream(location, flow=flow)
    start.set_downstream(heb_dt, flow=flow)
    start.set_downstream(str_opstmt_srt_cd, flow=flow)

    end_task = final_task(date_hour_path=date_hour_path)
    end_task.set_upstream(lin_of_bus, flow=flow)
    end_task.set_upstream(company, flow=flow)
    end_task.set_upstream(ck_coll_cntl, flow=flow)
    end_task.set_upstream(cmptr_typ, flow=flow)
    end_task.set_upstream(dist, flow=flow)
    end_task.set_upstream(ecomm_mkt_area, flow=flow)
    end_task.set_upstream(financial_div, flow=flow)
    end_task.set_upstream(frnt_end, flow=flow)
    end_task.set_upstream(location_typ, flow=flow)
    end_task.set_upstream(mkt_rgn, flow=flow)
    end_task.set_upstream(retl_loc_frmt, flow=flow)
    end_task.set_upstream(retl_loc_segm, flow=flow)
    end_task.set_upstream(retl_loc_stat_typ, flow=flow)
    end_task.set_upstream(retl_loc_typ, flow=flow)
    end_task.set_upstream(retl_unld, flow=flow)
    end_task.set_upstream(srs_sys, flow=flow)
    end_task.set_upstream(str_opstmt_hdr_typ, flow=flow)
    end_task.set_upstream(str_opstmt_typ, flow=flow)
    end_task.set_upstream(retail_loc, flow=flow)
    end_task.set_upstream(location, flow=flow)
    end_task.set_upstream(heb_dt, flow=flow)
    end_task.set_upstream(str_opstmt_srt_cd, flow=flow)

    #flow.run(parameters={"start": "hello"}, executor=DaskExecutor(cluster_kwargs={"silence_logs": 10}))
    #flow.environment.executor = DaskExecutor()
