def _prepareData(data, subtable=''):

    schemaMapper = [{"mongo": "string",
                     "redshift": "varchar(512)"},
                    {"mongo": "text",
                     "redshift": "varchar(6000)"},
                    {"mongo": "integer",
                     "redshift": "integer"},
                    {"mongo": "bigint",
                     "redshift": "bigint"},
                    {"mongo": "boolean",
                     "redshift": "boolean"},
                    {"mongo": "datetime",
                     "redshift": "datetime"},
                    {"mongo": "float",
                     "redshift": "float"},
                    {"mongo": "double",
                     "redshift": "float(53)"}]

    def projection(d, parent_key='', sep='_', subtable=''):
        projection_items = []

        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            alt_key = parent_key + '.' + k if parent_key else k
            if isinstance(v, dict):
                projection_items.extend(projection(v, new_key.replace(sep, '.'), sep=sep).items())
            elif isinstance(v, str):
                projection_items.append((new_key.replace('.', '_'), '${}'.format(alt_key)))
        return dict(projection_items)

    def schema(d, parent_key='', sep='_'):
        schema_items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                schema_items.extend(schema(v, new_key, sep=sep).items())
            elif isinstance(v, str):
                schema_items.append((new_key, v))
        return dict(schema_items)

    def _convertSchema(schema):
        output_array = []
        for k, v in schema.items():
            base_dict = dict()
            base_dict['name'] = k
            base_dict['type'] = v
            for mapping in schemaMapper:
                if mapping['mongo'] == v.lower():
                    base_dict['type'] = mapping['redshift']
            output_array.append(base_dict)
        return output_array

    return projection(data, subtable), _convertSchema(schema(data))
