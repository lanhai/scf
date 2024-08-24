<?php
namespace Scf\Mode\Native;


class Window {

    protected int $width = 900;
    protected int $height = 600;
    protected string $index = APP_PUBLIC_PATH . '/index.html';
    protected App $app;

    public function __construct($app) {
        $this->app = $app;
    }

    public static function bind($app): static {
        return new static($app);
    }

    /**
     * 参数可以是一个url
     * @param string|null $index
     * @return void
     */
    public function open(?string $index = null): void {
        $this->index = $index ?: $this->index;
        $this->app->createWidth($this);
    }

    public function index(string $index): static {
        $this->index = $index;
        return $this;
    }

    public function width(int $width): static {
        $this->width = $width;
        return $this;
    }

    public function height(int $heigth): static {
        $this->height = $heigth;
        return $this;
    }

    public function getWidth(): int {
        return $this->width;
    }

    public function getHeight(): int {
        return $this->height;
    }

    public function getIndex(): string {
        return $this->index;
    }
}