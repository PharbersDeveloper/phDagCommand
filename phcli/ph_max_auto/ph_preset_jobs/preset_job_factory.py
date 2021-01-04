from phcli.ph_errs.ph_err import exception_not_found_preset_job


def preset_factory(context, job_name, **kwargs):
    if 'preset.write_asset' == job_name:
        from phcli.ph_max_auto.ph_preset_jobs.write_asset import copy_job
        copy_job(context, **kwargs)
    else:
        raise exception_not_found_preset_job
