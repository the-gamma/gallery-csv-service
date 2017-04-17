#if INTERACTIVE
#r "../packages/Suave/lib/net40/Suave.dll"
#r "../packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#r "../packages/Newtonsoft.Json/lib/net40/Newtonsoft.Json.dll"
#load "../packages/FSharp.Azure.StorageTypeProvider/StorageTypeProvider.fsx"
#load "config.fs"
#load "pivot.fs"
#else
module Gallery.App
#endif
open System
open System.IO
open System.Collections.Generic
open Microsoft.WindowsAzure.Storage

open Suave
open Suave.Filters
open Suave.Writers
open Suave.Operators
open FSharp.Data
open Newtonsoft.Json


// --------------------------------------------------------------------------------------
// Data we store about CSV files
// --------------------------------------------------------------------------------------

type CsvFile = 
  { id : string 
    hidden : bool 
    date : DateTime
    source : string
    description : string
    tags : string[] 
    passcode : string }
  static member Create(id) = 
    { id = id; hidden = true; date = DateTime.Today
      source = ""; description = ""; tags = [||]; 
      passcode = System.Guid.NewGuid().ToString("N") }

// --------------------------------------------------------------------------------------
// Saving and reading CSV files
// --------------------------------------------------------------------------------------

#if INTERACTIVE
let createCloudBlobClient() = 
  let account = CloudStorageAccount.Parse(Config.TheGammaSnippetsStorage)
  account.CreateCloudBlobClient()
#else
let createCloudBlobClient() = 
  let account = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("CUSTOMCONNSTR_THEGAMMASNIPS_STORAGE"))
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

let uploadCsv count data =
  let container = createCloudBlobClient().GetContainerReference("uploads")
  if container.Exists() then
    let name = sprintf "%s/file_%d.csv" (System.DateTime.Now.ToString("yyyy-MM-dd")) count
    let blob = container.GetBlockBlobReference(name)
    if blob.Exists() then blob.Delete() // failwithf "Blob '%s' already exists!" name
    blob.UploadText(data, System.Text.Encoding.UTF8) 
    CsvFile.Create(name)
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

type ParsedFile = (string * string)[] * (string * obj)[][]

type Message = 
  | UploadFile of string * AsyncReplyChannel<CsvFile>
  | FetchFile of string * AsyncReplyChannel<option<ParsedFile>>

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
      | Some(UploadFile(data, repl)) ->
          let count = files.Values |> Seq.filter (fun f -> f.date = DateTime.Today) |> Seq.length
          let csv = uploadCsv count data
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
// Server with /upload endpoint and /csv for Pivot type provider
// --------------------------------------------------------------------------------------

let app =
  choose [
    GET >=> path "/" >=> Successful.OK "Service is running..." 
    POST >=> path "/update" >=> request (fun r ->
        let file = fromJson<CsvFile> (System.Text.Encoding.UTF8.GetString(r.rawForm))
        printfn "%A" file
        Successful.OK ""
      )
    POST >=> path "/upload" >=> (fun ctx -> async { 
      let data = System.Text.Encoding.UTF8.GetString(ctx.request.rawForm)
      try 
        ignore (readCsvFile data) 
        let! file = cache.PostAndAsyncReply(fun ch -> UploadFile(data, ch))
        return! Successful.OK (toJson file) ctx
      with ParseError msg -> 
        return! RequestErrors.BAD_REQUEST msg ctx })

    setHeader  "Access-Control-Allow-Origin" "*"
    >=> setHeader "Access-Control-Allow-Headers" "content-type"
    >=> choose [
      OPTIONS >=> Successful.OK "CORS approved"
      GET >=> pathScan "/csv/%s" (fun source ctx -> async {
        let! file = cache.PostAndAsyncReply(fun ch -> FetchFile(source, ch))
        match file with 
        | None -> return! RequestErrors.BAD_REQUEST (sprintf "File with id '%s' does not exist!" source) ctx
        | Some (meta, data) -> return! handleRequest meta data (List.map fst ctx.request.query) ctx }) ]
  ]


// When port was specified, we start the app (in Azure), 
// otherwise we do nothing (it is hosted by 'build.fsx')
match System.Environment.GetCommandLineArgs() |> Seq.tryPick (fun s ->
    if s.StartsWith("port=") then Some(int(s.Substring("port=".Length)))
    else None ) with
| Some port ->
    let serverConfig =
      { Web.defaultConfig with
          logger = Logging.Loggers.saneDefaultsFor Logging.LogLevel.Debug
          bindings = [ HttpBinding.mkSimple HTTP "127.0.0.1" port ] }
    Web.startWebServer serverConfig app
| _ -> ()



