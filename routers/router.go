package routers

import (
	"github.com/astaxie/beego"
	"github.com/someone235/gett/controllers"
)

func init() {
	beego.Router("/", &controllers.MainController{})
}
