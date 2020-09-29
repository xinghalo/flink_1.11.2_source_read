import { ComponentFactory, ComponentFactoryResolver, ElementRef, Renderer2, ViewContainerRef } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzTooltipDirective } from '../tooltip/nz-tooltip.directive';
import { NzPopoverComponent } from './nz-popover.component';
export declare class NzPopoverDirective extends NzTooltipDirective {
    noAnimation?: NzNoAnimationDirective | undefined;
    factory: ComponentFactory<NzPopoverComponent>;
    constructor(elementRef: ElementRef, hostView: ViewContainerRef, resolver: ComponentFactoryResolver, renderer: Renderer2, tooltip: NzPopoverComponent, noAnimation?: NzNoAnimationDirective | undefined);
}
