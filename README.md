# Integrating SQLModel, Datasette (SQLite_Utils) and Holoviz Panel

This is a proof-of-concept / draft repository.

My name for this project is 'BeeDiscovery': a shortform for 'Better eDiscovery', primarily motivated by the inefficiencies I have found when attempting to validate tabular databases which we have recieved from other parties, and as a standardizing tool for my forensic and ediscovery team to massage and impose some semblance of order on the various datatypes that we encounter during all stages of cybersecurity incident response and our expert testimony work.

End Goal: ideally we can easily and repeatably dump data sources into sqlite (either through our own loaders or by leveraging existing loader interfaces which can dump to pandas / sqlite) and then triage the columns, assign roles and kick-off downstream processes.


__Future ideas:__

 - have a function which can generate datasette metadata .json objects: https://docs.datasette.io/en/stable/metadata.html
 - functionality to create / dump / restore ArangoDB collections to SQLite for long-term persistence.

__Environment Setup__

I'm using poetry to manage dependencies.
To get started, you will probably want to configure poetry to set up it's .env within the project, so your IDE can automatically find it. Run:

`poetry config virtualenvs.in-project true`

Then,

`poetry install`

Once that's done, recreate your bash session and, if you're not already running with the new environment through your IDE, you can run `poetry shell` to get a session within the correct environment.

Finally, run jupyter to get started: `jupyter lab --ip 0.0.0.0`



__Models__

 - *Dataset*
    - As many as you want in a single sqlite database
    - link to a `sqlite_utils` Table object for a table named for the `db_name` attribute
    - Future: if ArangoDB is enabled in the environment, also set up connection to a data collection and graph when the attributes are first called.
 - *DataField*
    - attached to a Dataset, describes metadata about a column. Generally created by reading data from the source table, using `Dataset.sync_columns()`
 - *DataRole*
    - describes how the contents of a column can be used. More of a tag on a column, eventually there will be validators and other checks.
    - processes should ideally be written referring to DataRoles, not DataFields, since the fields can change from Dataset-to-Dataset, but Roles are eternal.
 - *DataFieldRoleLink*: m2m link with a priority attribute (WIP)
    - just a class used by SQLModel/SQLAlchemy for the m2m relationship.
    - also has a `priority` attribute, but not used. Eventually may be used for sorting.
 - *BeeDiscovery*: general settings class, generator for new Datasets, stores the SQLAlchemy `._session` and all objects associated with a given .sqlite database.




__Helper Classes__

I have done some experimentation with in the helpers.py module, in `DynamicAttrDefaultDictList`, which takes in a list or dict and supports IDE autocompletion generated from attribute values. This is pretty convenient when working on flexible data tables, where you aren't always certain what columns are there, or don't want to type them in all the time in `['item']` notation.

This is also nice be because, once a DataRole has been assigned to a DataField (for example, the DOCDATE), you can write repeatable processes which refer to the `dataset.r.DOCDATE` and it will always know what you're referring to. You can thus write simple scripts / validations without needing to always map back to the specific columns in the target datasets.


