import {
  CompleteMultipartUploadCommandOutput,
  GetObjectCommand,
  GetObjectCommandInput,
  GetObjectCommandOutput,
  PutObjectCommand,
  PutObjectCommandInput,
  PutObjectCommandOutput,
  S3Client as client,
} from "@aws-sdk/client-s3";

import { Options, Upload } from "@aws-sdk/lib-storage";

let internalClient = new client({});

type S3Client = typeof internalClient & {
  get: (props: GetObjectCommandInput) => Promise<GetObjectCommandOutput>;
  put: (props: PutObjectCommandInput) => Promise<PutObjectCommandOutput>;
  uploadStream: <T>(props: Omit<Options, "client">) => Promise<CompleteMultipartUploadCommandOutput>;
};
export const s3Client = internalClient as S3Client;
s3Client.get = function (props: GetObjectCommandInput) {
  return s3Client.send(new GetObjectCommand(props));
};
s3Client.put = function (props: PutObjectCommandInput) {
  return s3Client.send(new PutObjectCommand(props));
};
s3Client.uploadStream = async function (props) {
  const uploader = new Upload({
    ...props,
    client: s3Client,
  });

  return uploader.done();
};
