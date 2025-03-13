-- Languages Dimension Table
CREATE OR REPLACE TABLE `sada-tirayr.kstack.Languages` (
    language_id INT64,
    language_name STRING
)
;

-- Repositories Dimension Table
CREATE OR REPLACE TABLE `sada-tirayr.kstack.Repositories` (
    repo_id INT64,
    language_id INT64,
    owner STRING,
    name STRING,
    is_fork BOOL,
    forks INT64,
    stars INT64,
    issues FLOAT64,
    license STRING
)
PARTITION BY RANGE_BUCKET(stars, GENERATE_ARRAY(0, 100000, 10000))
CLUSTER BY owner
;

-- Files Fact Table
CREATE OR REPLACE TABLE `sada-tirayr.kstack.Files` (
    file_id INT64,
    repo_id INT64,
    path STRING,
    content STRING,
    size INT64,
    commit_sha STRING,
    languages_distribution STRING
)
PARTITION BY RANGE_BUCKET(size, GENERATE_ARRAY(0, 1000000, 100000))
CLUSTER BY repo_id
;

ALTER table `sada-tirayr.kstack.Languages` ADD primary key(language_id) NOT ENFORCED;

ALTER table `sada-tirayr.kstack.Repositories` ADD primary key(repo_id) NOT ENFORCED,
ADD FOREIGN KEY(language_id) references kstack.Languages(language_id) NOT ENFORCED
;
ALTER table `sada-tirayr.kstack.Files` ADD primary key(file_id) NOT ENFORCED,
ADD FOREIGN KEY(repo_id) references `sada-tirayr.kstack.Repositories`(repo_id) NOT ENFORCED
;

