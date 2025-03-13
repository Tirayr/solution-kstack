CREATE VIEW `resounding-keel-378411.kstack.kstack_combined_view` AS
      SELECT fl.path,
            rp.owner,
            rp.repo_id,
            rp.is_fork,
            fl.languages_distribution,
            fl.content,
            rp.issues,
            lg.language_name AS main_language,
            rp.forks,
            rp.stars,
            fl.commit_sha,
            fl.size,
            rp.name,
            rp.license
      FROM      `resounding-keel-378411.kstack.Files`        AS fl
      LEFT JOIN `resounding-keel-378411.kstack.Repositories` AS rp ON fl.repo_id=rp.repo_id
      LEFT JOIN `resounding-keel-378411.kstack.Languages`    AS lg  ON rp.language_id=lg.language_id