// Add new function mappings for specific cases
{
  hiveName: 'json_tuple',
  snowflakeName: 'parse_json',
  transform: (args) => {
    const [json, ...paths] = args;
    return paths.map(path => `get_path(parse_json(${json}), '${path}')`).join(', ');
  }
},
{
  hiveName: 'get_json_object',
  snowflakeName: 'get_path',
  transform: (args) => `get_path(parse_json(${args[0]}), ${args[1]})`
}