package main

import (
	"fmt"
	"strconv"

	"github.com/astaxie/beego"
	"github.com/astaxie/beego/context"
	"github.com/someone235/gett/dbStuff"
	// _ "github.com/someone235/gett/routers"
)

func main() {
	dbStuff.Populate()
	fmt.Println("Populating is done!")

	beego.Get("/driver/get_all", func(ctx *context.Context) {
		d := dbStuff.GetAllDrivers()
		ctx.Output.JSON(d, false, false)
	})

	beego.Put("/driver", func(ctx *context.Context) {
		b := ctx.Input.CopyBody(1024 * 1024 * 1024)
		id := dbStuff.AddDriver(b)
		ctx.Output.JSON(id, false, false)
	})

	beego.Get("/driver/:id", func(ctx *context.Context) {
		driverId, err := strconv.ParseInt(ctx.Input.Param(":id"), 10, 64)
		if err != nil {
			panic(err)
		}
		d := dbStuff.GetDriverById(int(driverId))
		ctx.Output.JSON(d, false, false)
	})

	beego.Patch("/driver/:id", func(ctx *context.Context) {
		driverId, err := strconv.ParseInt(ctx.Input.Param(":id"), 10, 64)
		if err != nil {
			panic(err)
		}
		b := ctx.Input.CopyBody(1024 * 1024 * 1024)
		dbStuff.UpdateDriver(int(driverId), b)
		sendSuccess(ctx)
	})

	beego.Delete("/driver/:id", func(ctx *context.Context) {
		driverId, err := strconv.ParseInt(ctx.Input.Param(":id"), 10, 64)
		if err != nil {
			panic(err)
		}
		dbStuff.DeleteDriver(int(driverId))
		sendSuccess(ctx)
	})

	beego.Put("/metric", func(ctx *context.Context) {
		b := ctx.Input.CopyBody(1024 * 1024 * 1024)
		err := dbStuff.AddMetric(b)
		if err != nil {
			sendError(ctx, err)
		} else {
			sendSuccess(ctx)
		}
	})

	beego.Delete("/metric/:id", func(ctx *context.Context) {
		metricId, err := strconv.ParseInt(ctx.Input.Param(":id"), 10, 64)
		if err != nil {
			panic(err)
		}
		dbStuff.DeleteMetric(int(metricId))
		sendSuccess(ctx)
	})

	beego.Get("/max_matric/:metric_name", func(ctx *context.Context) {
		d := dbStuff.GetMaxMetric(ctx.Input.Param(":metric_name"))
		ctx.Output.JSON(d, false, false)
	})

	beego.Run()
}

func sendSuccess(ctx *context.Context) {
	ctx.Output.JSON(struct {
		Result string `json:"result"`
	}{"ok"}, false, false)
}

func sendError(ctx *context.Context, err error) {
	ctx.Output.SetStatus(400)
	ctx.Output.JSON(struct {
		ErrorMsg string `json:"errorMsg"`
	}{err.Error()}, false, false)
}
