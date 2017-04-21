﻿module Gallery.CsvService.Storage

open System
open System.Collections.Generic
open Newtonsoft.Json
open Microsoft.WindowsAzure.Storage

// --------------------------------------------------------------------------------------
// Data we store about CSV files
// --------------------------------------------------------------------------------------

type CsvFile = 
  { id : string 
    hidden : bool 
    date : DateTime
    source : string
    title : string
    description : string
    tags : string[] 
    passcode : string }
  static member Create(id) = 
    { id = id; hidden = true; date = DateTime.Today
      title = ""; source = ""; description = ""; tags = [||]; 
      passcode = System.Guid.NewGuid().ToString("N") }

// --------------------------------------------------------------------------------------
// Saving and reading CSV files
// --------------------------------------------------------------------------------------

#if INTERACTIVE
let createCloudBlobClient() = 
  let account = CloudStorageAccount.Parse(Config.TheGammaGalleryCsvStorage)
  account.CreateCloudBlobClient()
#else
let createCloudBlobClient() = 
  let account = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("CUSTOMCONNSTR_THEGAMMACSV_STORAGE"))
  account.CreateCloudBlobClient()
#endif

let serializer = JsonSerializer.Create()

let toJson value = 
  let sb = System.Text.StringBuilder()
  use tw = new System.IO.StringWriter(sb)
  serializer.Serialize(tw, value)
  sb.ToString() 

let fromJson<'R> str : 'R = 
  use tr = new System.IO.StringReader(str)
  serializer.Deserialize(tr, typeof<'R>) :?> 'R

let generateId (date:System.DateTime) i = 
  sprintf "%s/file_%d.csv" (date.ToString("yyyy-MM-dd")) i

let uploadCsv id data =
  let container = createCloudBlobClient().GetContainerReference("uploads")
  if container.Exists() then
    let blob = container.GetBlockBlobReference(id)
    if blob.Exists() then blob.Delete() // failwithf "Blob '%s' already exists!" name
    blob.UploadText(data, System.Text.Encoding.UTF8) 
    CsvFile.Create(id)
  else failwith "Container 'uploads' not found"

let downloadCsv csv =
  let container = createCloudBlobClient().GetContainerReference("uploads")
  if container.Exists() then
    let blob = container.GetBlockBlobReference(csv.id)
    if not (blob.Exists()) then None
    else Some(blob.DownloadText(System.Text.Encoding.UTF8))
  else failwith "Container 'uploads' not found"

let readMetadata () =
  let container = createCloudBlobClient().GetContainerReference("uploads")
  if container.Exists() then
    let blob = container.GetBlockBlobReference("files.json")
    if blob.Exists() then 
      blob.DownloadText(System.Text.Encoding.UTF8) |> fromJson<CsvFile[]> 
    else failwith "Blob 'files.json' does not exist."
  else failwith "Container 'uploads' not found" 

let writeMetadata (files:CsvFile[]) = 
  let json = files |> toJson
  let container = createCloudBlobClient().GetContainerReference("uploads")
  if container.Exists() then
    let blob = container.GetBlockBlobReference("files.json")
    blob.UploadText(json, System.Text.Encoding.UTF8)
  else failwith "container 'uploads' not found" 

// --------------------------------------------------------------------------------------
// Keep list of CSV files and cache recently accessed
// --------------------------------------------------------------------------------------

open Gallery.CsvService.Pivot

type ParsedFile = (string * string)[] * (string * Value)[][]

type Message = 
  | UploadFile of string * AsyncReplyChannel<CsvFile>
  | FetchFile of string * AsyncReplyChannel<option<ParsedFile>>
  | UpdateRecord of CsvFile
  | GetRecords of AsyncReplyChannel<CsvFile[]>

let cache = MailboxProcessor.Start(fun inbox ->
  let worker () = async {
    let cache = new Dictionary<_, DateTime * _>()
    let files = new Dictionary<_, _>()
    for f in readMetadata () do files.Add(f.id, f)

    while true do
      let! msg = inbox.TryReceive(timeout=1000*60)
      let remove = [ for (KeyValue(k, (t, _))) in cache do if (DateTime.Now - t).TotalMinutes > 5. then yield k ]
      for k in remove do cache.Remove(k) |> ignore
      match msg with
      | None -> ()
      | Some(GetRecords ch) ->
          ch.Reply (Array.ofSeq files.Values)

      | Some(UpdateRecord(file)) ->
          if files.ContainsKey(file.id) && files.[file.id].passcode = file.passcode then
            files.[file.id] <- file
            writeMetadata (Array.ofSeq files.Values)

      | Some(UploadFile(data, repl)) ->
          let id = Seq.initInfinite (generateId DateTime.Today) |> Seq.filter (files.ContainsKey >> not) |> Seq.head
          let csv = uploadCsv id data
          files.Add(csv.id, csv)
          writeMetadata (Array.ofSeq files.Values)
          repl.Reply(csv)

      | Some(FetchFile(id, repl)) ->
          if not (files.ContainsKey id) then repl.Reply(None) else
          if not (cache.ContainsKey id) then
              match downloadCsv files.[id] with
              | Some data -> cache.Add(id, (DateTime.Now, readCsvFile data))
              | None -> ()
          match cache.TryGetValue id with
          | true, (_, res) -> 
              cache.[id] <- (DateTime.Now, res)
              repl.Reply(Some res)
          | _ -> repl.Reply None }
  async { 
    while true do
      try return! worker ()
      with e -> printfn "Agent failed: %A" e })

// --------------------------------------------------------------------------------------
// Server components
// --------------------------------------------------------------------------------------

open Suave

let fetchFile source = 
  cache.PostAndAsyncReply(fun ch -> FetchFile(source, ch))

let getRecords () =
  cache.PostAndAsyncReply(GetRecords) 

let updateRecord = request (fun r ->
  let file = fromJson<CsvFile> (System.Text.Encoding.UTF8.GetString(r.rawForm))
  cache.Post(UpdateRecord file)
  Successful.OK "" )

let uploadFile = (fun ctx -> async { 
  let data = System.Text.Encoding.UTF8.GetString(ctx.request.rawForm)
  try 
    ignore (readCsvFile data) 
    let! file = cache.PostAndAsyncReply(fun ch -> UploadFile(data, ch))
    return! Successful.OK (toJson file) ctx
  with ParseError msg -> 
    return! RequestErrors.BAD_REQUEST msg ctx })
