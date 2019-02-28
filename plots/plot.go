package plots

import (
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func Plt(data map[string][]float64, sizes []int, title, xlabel, ylabel string) {
	p, err := plot.New()
	if err != nil {
		panic(err)
	}

	// p.Title.Text = title
	p.X.Label.Text = xlabel
	p.Y.Label.Text = ylabel

	err = plotutil.AddLinePoints(p, "hdrf", points(sizes, data["hdrf"]),
		"eti", points(sizes, data["eti"]), "hash", points(sizes, data["hash"]))
	if err != nil {
		panic(err)

	}

	// Save the plot to a PNG file.
	if err := p.Save(4*vg.Inch, 4*vg.Inch, title+".png"); err != nil {
		panic(err)
	}
}

func points(x []int, y []float64) plotter.XYs {
	pts := make(plotter.XYs, len(x))
	for i := range pts {
		pts[i].X = float64(x[i])
		pts[i].Y = y[i]
	}
	return pts
}
