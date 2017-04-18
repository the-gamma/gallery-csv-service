module Gallery.CsvService.Listing
open Gallery.CsvService

open Suave
open Suave.Filters
open Suave.Operators
open System
open Gallery.CsvService.Storage

let getTagId (s:string) =
  let rec loop (sb:System.Text.StringBuilder) dash i = 
    if i = s.Length then sb.ToString()
    elif Char.IsLetterOrDigit s.[i] then loop (sb.Append(Char.ToLower s.[i])) false (i+1)
    elif dash then loop sb true (i+1)
    else loop (sb.Append '-') true (i+1)
  loop (System.Text.StringBuilder()) true 0

let handleRequest (files:CsvFile[]) = 
  let tags = files |> Seq.collect (fun f -> f.tags) |> Seq.map (fun t -> getTagId t, t) |> dict
  choose [
    path "/providers/listing/" >=> request (fun r ->
      Serializer.returnMembers [        
        for (KeyValue(tid, t)) in tags ->
          Member.Property(t, Result.Nested("tag/" + tid + "/"), [])
      ])
    pathScan "/providers/listing/tag/%s/" (fun tid ->
      Serializer.returnMembers [
        for file in files do
          let hasTag = file.tags |> Seq.exists (fun t -> getTagId t = tid)
          if hasTag then 
            yield Member.Property(file.title, Result.Nested("/providers/listing/" + tid), [])
      ])
  ]