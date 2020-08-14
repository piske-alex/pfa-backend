// Learn more about F# at http://fsharp.org

open System
open oxen
open StackExchange.Redis
open Suave
open System.Threading
open dotenv.net

DotEnv.Config()

type QueueData = {
    value: string
}

let inline (~~) (x:^a) : ^b = ((^a or ^b) : (static member op_Implicit: ^a -> ^b) x)

let redis = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable("REDIS_HOST"));


let readRedis = async {
    while true do
        let db = redis.GetDatabase();
        Console.WriteLine("finding unnoted rewards")
        let job = db.ListRightPopLeftPush(~~"queue", ~~"processing")
        if job.IsNullOrEmpty then do! Async.Sleep(30000)
        else 
            Console.WriteLine("processing")
            Rewards.getReward (Rewards.getOrder (job.ToString()))
            db.ListLeftPop(~~"processing")
            //match job.ToString().Split(';').[0] with
            //| "reward" -> fun _ -> (Rewards.getReward (Rewards.getOrder (job.ToString().Split(';').[1]))
            //                        db.ListLeftPop(~~"processing"))
    0
    }

open Suave.Filters
open Suave.Operators
open Suave.Successful
open Suave.Utils.Collections
open System.Net

[<EntryPoint>]
let main argv =
    Async.StartAsTask readRedis |> ignore
    let cts = new CancellationTokenSource()
    let conf = { defaultConfig with cancellationToken = cts.Token; bindings = [ HttpBinding.create HTTP IPAddress.Loopback 800us 
                                                                                HttpBinding.createSimple HTTP (Environment.GetEnvironmentVariable("HOST")) (Int32.Parse (Environment.GetEnvironmentVariable("PORT")))] }
    // let getVIPLevel q = defaultArg (Option.ofChoice (q ^^ "name")) "World" |> sprintf "Hello %s"

    let allowHeaders = 
        Writers.setHeader "Access-Control-Allow-Credentials" "true" >=>
        Writers.setHeader "Access-Control-Allow-Headers" "*" >=>
        Writers.setHeader "Access-Control-Allow-Methods" "GET, POST, PUT, DELETE, OPTIONS" >=>
        Writers.setHeader "Access-Control-Allow-Origin" "*"

    let getVIPLevel address :WebPart = 
        address |> Rewards.getUserTotalSpent |> Rewards.resolveSums |> 
        Rewards.getVIPPolicy |> fun (x:Result<Rewards.VIPPolicy list,exn>) -> (match x with
                                                                |Ok req -> allowHeaders >=> Writers.setMimeType "application/json; charset=utf-8" >=> OK (req.[0] |> Json.toJson |> System.Text.Encoding.UTF8.GetString) 
                                                                |Error e -> Suave.RequestErrors.NOT_FOUND e.Message)

    let getCommunityReward address :WebPart =
        address |> Rewards.getUserReferralTotalReward |> Rewards.resolveSums |> Rewards.getCommunityPolicy |> fun (x:Result<Rewards.VIPPolicy list,exn>) -> (
            match x with
            |Ok req -> req |> fun (req:Rewards.VIPPolicy list) -> allowHeaders >=> Writers.setMimeType "application/json; charset=utf-8" >=> OK (req.[0] |> Json.toJson |> System.Text.Encoding.UTF8.GetString) 
            |Error e -> Suave.RequestErrors.NOT_FOUND e.Message
        )
        
    

    let app =
        choose [ 
            GET >=> 
                choose [ 
                    path "/hello" >=> OK "Hello World!"
                    pathScan "/vipLevel/%s" getVIPLevel
                    pathScan "/communityReward/%s" getCommunityReward ]
            OPTIONS >=> 
            choose [ 
                path "/hello" >=> OK "Hello World!"
                pathScan "/vipLevel/%s" getVIPLevel
                pathScan "/communityReward/%s" getCommunityReward ]
            Suave.RequestErrors.NOT_FOUND "Not found" ]
    let listening, server = startWebServerAsync conf app
      
    Async.Start(server, cts.Token)
    printfn "Make requests now"
    Console.ReadLine() |> ignore
    cts.Cancel()
    0 // return an integer exit code
