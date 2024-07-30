from google.cloud import bigquery

def create_schema_fields(schema):
    def create_field(field):
        # Default mode and description if not provided
        mode = field.get('mode', 'NULLABLE')
        description = field.get('description', None)

        if field['type'] == 'RECORD':
            # Recursively process sub-fields
            sub_fields = [
                create_field(sub_field)
                for sub_field in field.get('fields', [])
            ]
            return bigquery.SchemaField(
                field['name'],
                'RECORD',
                mode=mode,
                description=description,
                fields=sub_fields
            )
        else:
            return bigquery.SchemaField(
                field['name'],
                field['type'],
                mode=mode,
                description=description
            )

    return [create_field(field) for field in schema]
