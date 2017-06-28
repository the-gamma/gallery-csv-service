module WebScrape.DataProviders

open System
open FSharp.Data
open FSharp.Data.Runtime

type ParsingContext = 
    { Headings : Map<string, string> 
      Rows : list<string[]> }

type ExploreTree = CsvProvider<"Heading 2, Heading 3, Item">
type ExploreDate = CsvProvider<"Type, Date, Entry">

let getTree url =
  let page = Http.RequestString(url, responseEncodingOverride="UTF-8")
  let doc = HtmlDocument.Parse(page)

  let specialHeadings = set ["h2";"h3"]
  let leafElements = set ["li"]

  let rec visitElement ctx (el:HtmlNode) =
    let name = el.Name()
    if specialHeadings.Contains name then
      { ctx with Headings = Map.add name (el.InnerText()) ctx.Headings }
    elif leafElements.Contains name then
      let row = [ for head in specialHeadings -> defaultArg (ctx.Headings.TryFind(head)) "" ]
      let row = row @ [ el.InnerText() ] |> Array.ofList
      { ctx with Rows = row::ctx.Rows }
    else
      el.Elements() |> Seq.fold visitElement ctx

  let emptyCtx = { Headings = Map.empty; Rows = [] }
  doc.Elements() |> Seq.fold visitElement emptyCtx 
 
let getAllEntries (url:string) =
  let resCtx = getTree url
  let csv = 
    [ for row in resCtx.Rows -> ExploreTree.Row(row.[0], row.[1], row.[2]) ]
    |> ExploreTree.GetSample().Append
  csv

let isMonth (monthName:string) =
    let months = ["January";"February";"March";"April";"May";"June";"July";"August";"September";"October";"November";"December"]
    let matchedMonth = months |> List.filter(fun x -> x.StartsWith(monthName))
    if (matchedMonth.IsEmpty) then
      None
    else Some(matchedMonth.[0]) 

let isDated (entry: string) =
  let listOfWords = entry.Split(' ')
  if (listOfWords.Length > 4) then 
    let could, validDated = System.DateTime.TryParse(String.concat " " listOfWords.[0..1])
    could  
  else
    false

let getDated (entries:string[] list) = 
  let datedEntries = entries |> List.map (fun entry -> 
                                            if (isDated entry.[2]) then
                                              let listOfWords = entry.[2].Split(' ')
                                              let could1, option1 = System.Int32.TryParse(listOfWords.[0])
                                              if could1 then
                                                let dd = listOfWords.[0]
                                                let monthName = isMonth listOfWords.[1]
                                                match monthName with
                                                | Some x ->
                                                  let mm = x
                                                  let yyyy = "2015"
                                                  let description = listOfWords.[3..] |> String.concat " " 
                                                  let entryDateString = sprintf "%s/%s/%s" dd mm yyyy
                                                  let could, entryDate = System.DateTime.TryParse(entryDateString)
                                                  if could then 
                                                    [entry.[0]; entryDate.ToString(); description]
                                                  else 
                                                    // printfn "Error1: %A" entryDateString 
                                                    []
                                                | None -> 
                                                    // printfn "Error2: %A" entry 
                                                    []
                                              else 
                                                let could2, option2 = System.Int32.TryParse(listOfWords.[1]) 
                                                if could2 then
                                                  let dd = listOfWords.[1]
                                                  let monthName = isMonth listOfWords.[0]
                                                  match monthName with
                                                  | Some x ->
                                                    let mm = x
                                                    let yyyy = "2015"
                                                    let description = listOfWords.[3..] |> String.concat " " 
                                                    let entryDateString = sprintf "%s/%s/%s" dd mm yyyy
                                                    let could, entryDate = System.DateTime.TryParse(entryDateString)
                                                    if could then 
                                                      [entry.[0]; entryDate.ToString(); description]
                                                    else 
                                                      // printfn "Error1: %A" entryDateString 
                                                      []
                                                  | None -> 
                                                      // printfn "Error2: %A" entry 
                                                      []
                                                else
                                                  // printfn "Error3: %A" entry  
                                                  []
                                            else 
                                              // printfn "Error4: %A" entry  
                                              []
                                          )
  datedEntries
  
let getDatedEntries (url:string) =
  let resCtx = getTree url
  let datedEntries = getDated resCtx.Rows
  let removedEmpties = datedEntries |> List.filter (fun entry -> not (List.isEmpty entry))
  let csv = 
    [ for entry in removedEmpties -> ExploreDate.Row(entry.[0], entry.[1], entry.[2])]
    |> ExploreDate.GetSample().Append
  csv