import {
  DynamoDBDocumentClient,
  GetCommand,
  GetCommandInput,
  GetCommandOutput,
  QueryCommand,
  QueryCommandInput,
  QueryCommandOutput,
  TransactWriteCommand,
  TransactWriteCommandInput,
  UpdateCommand,
  UpdateCommandInput,
  UpdateCommandOutput,
} from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { Money } from "@microws/types";

if (process.env.npm_package_config_microws_awsProfile) {
  process.env.AWS_PROFILE = process.env.npm_package_config_microws_awsProfile;
}
type DynamoCustomAddOns = {
  get: <T extends Record<string, any>>(
    params: Omit<GetCommandInput, "TableName"> & {
      TableName?: string;
    },
  ) => Promise<
    Omit<GetCommandOutput, "Item"> & {
      Item: T;
    }
  >;
  query: <T extends Record<string, any>>(
    params: Omit<QueryCommandInput, "TableName"> & {
      TableName?: string;
    },
  ) => Promise<
    Omit<QueryCommandOutput, "Items"> & {
      Items: Array<T>;
    }
  >;
  save: <T extends Record<string, any>>(
    params:
      | (Omit<UpdateCommandInput, "TableName"> & {
          TableName?: string;
        })
      | (Omit<UpdateCommandInput, "TableName"> & {
          TableName?: string;
          row: Partial<T>;
        }),
  ) => Promise<
    UpdateCommandOutput & {
      Item: T;
    }
  >;
};
const dynamodb: DynamoDBDocumentClient & Partial<DynamoCustomAddOns> = DynamoDBDocumentClient.from(
  new DynamoDBClient({}),
  {
    marshallOptions: {
      convertEmptyValues: true,
      removeUndefinedValues: true,
    },
  },
);

//This should be used for low throughput tables only, where you want to keep the order but
//don't have something like kinesis to do it for you
export async function dynamoOrderedWrite<T>(
  get: GetCommand,
  func: (current: T) => Promise<TransactWriteCommandInput["TransactItems"]>,
) {
  let count = 10;
  let success = false;
  do {
    try {
      let { Item: current } = (await dynamodb.send(get)) as { Item: T };
      await dynamodb.send(
        new TransactWriteCommand({
          TransactItems: await func(current),
        }),
      );
      success = true;
    } catch (e) {
      console.log(e);
      count--;
    }
  } while (!success && count > 0);
  if (!success) {
    throw new Error("Gave Up");
  }
}
//Seems to be an issue with their typing for transactWriteItems, fixing.
type UpdateOverride = UpdateCommandInput & {
  UpdateExpression: string;
};
export function CreateDynamoDBUpdateParams(
  params: Omit<UpdateCommandInput, "TableName"> & {
    row?: NodeJS.Dict<any>;
    TableName?: string;
  },
): UpdateOverride {
  if (!params.TableName) {
    params.TableName = process.env.MAIN_TABLE;
  }
  if (!params.ExpressionAttributeNames) {
    params.ExpressionAttributeNames = {};
  }
  if (!params.ExpressionAttributeValues) {
    params.ExpressionAttributeValues = {};
  }
  if (!params.UpdateExpression) {
    params.UpdateExpression = "";
  }
  if (params.row) {
    if (!params.UpdateExpression.includes("SET ")) {
      params.UpdateExpression += ` SET `;
    }
    params.UpdateExpression += ` ${Object.entries(params.row)
      .map(([key, value]) => {
        if (key in params.Key) {
          return;
        }
        const newKey = key.replace(/[^a-zA-Z0-9]/g, "_");

        params.ExpressionAttributeNames[`#a_${newKey}`] = key;
        params.ExpressionAttributeValues[`:a_${newKey}`] = value == undefined ? null : value;

        return `#a_${newKey}=:a_${newKey}`;
      })
      .filter(Boolean)
      .join(", ")}`;
  }
  return params as UpdateOverride;
}
dynamodb.query = async function get<T extends Record<string, any>>(params: QueryCommandInput) {
  if (!params.TableName) {
    params.TableName = process.env.MAIN_TABLE;
  }
  try {
    return (await dynamodb.send(new QueryCommand(params))) as Omit<QueryCommandOutput, "Items"> & {
      Items: Array<T>;
    };
  } catch (e) {
    throw new Error(e.message);
  }
};
dynamodb.get = async function get<T extends Record<string, any>>(params: GetCommandInput) {
  if (!params.TableName) {
    params.TableName = process.env.MAIN_TABLE;
  }
  try {
    return (await dynamodb.send(new GetCommand(params))) as Omit<GetCommandOutput, "Item"> & {
      Item: T;
    };
  } catch (e) {
    throw new Error(e.message);
  }
};
dynamodb.save = async function save<T extends Record<string, any>>(
  params:
    | (Omit<UpdateCommandInput, "TableName"> & {
        TableName?: string;
      })
    | (Omit<UpdateCommandInput, "TableName"> & {
        row?: NodeJS.Dict<any>;
        TableName?: string;
      }),
) {
  if (!params.TableName) {
    params.TableName = process.env.MAIN_TABLE;
  }
  if (!params.ReturnValues) {
    params.ReturnValues = "ALL_NEW";
  }

  let updateStatement: UpdateCommand;
  if ("row" in params) {
    updateStatement = new UpdateCommand(CreateDynamoDBUpdateParams(params));
  } else {
    updateStatement = new UpdateCommand(params as UpdateCommandInput);
  }
  try {
    let result: UpdateCommandOutput = await dynamodb.send(updateStatement);
    return {
      ...result,
      Item: result.Attributes as T,
    };
  } catch (e) {
    throw new Error(e.message);
  }
};
function reverseEpoch(date) {
  return 32503680000000 - date.valueOf();
}

type SortInput = string | Date | boolean;
function sortAddon(sort: SortInput) {
  if (sort === true) {
    return "_" + reverseEpoch(new Date()).toString().padStart(16, "0");
  } else if (sort instanceof Date) {
    return "_" + reverseEpoch(sort).toString().padStart(16, "0");
  } else if (sort) {
    return "_" + sort;
  } else {
    return "";
  }
}
export const AutoCompleteFormatter = {
  keyword: (text: string, sort?: SortInput) =>
    text
      ?.toString()
      ?.normalize("NFD")
      ?.replace(/\p{Diacritic}/gu, "")
      ?.toLowerCase()
      ?.replace(/[^a-zA-Z0-9]*/g, "") + sortAddon(sort),
  date: (date: Date, sort?: SortInput) => AutoCompleteFormatter.keyword(date.toISOString(), sort),
  money: (amount: Money, sort?: SortInput) => amount.toString().padStart(16, "0") + sortAddon(sort),
  short6Id: (text: string, sort?: SortInput) => AutoCompleteFormatter.keyword(text.slice(-6), sort),
};

const externaldynamodb: DynamoDBDocumentClient & DynamoCustomAddOns = dynamodb as any;

export { externaldynamodb as dynamodb };
