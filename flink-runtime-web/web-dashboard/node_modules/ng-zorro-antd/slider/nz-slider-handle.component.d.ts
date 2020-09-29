import { ChangeDetectorRef, OnChanges, OnDestroy, SimpleChanges } from '@angular/core';
import { NGStyleInterface } from '../core/types/ng-class';
import { NzToolTipComponent } from '../tooltip/nz-tooltip.component';
import { SliderShowTooltip } from './nz-slider-definitions';
import { NzSliderComponent } from './nz-slider.component';
export declare class NzSliderHandleComponent implements OnChanges, OnDestroy {
    private sliderComponent;
    private cdr;
    tooltip: NzToolTipComponent;
    nzVertical: string;
    nzOffset: number;
    nzValue: number;
    nzTooltipVisible: SliderShowTooltip;
    nzTipFormatter: (value: number) => string;
    nzActive: boolean;
    tooltipTitle: string;
    style: NGStyleInterface;
    private hovers_;
    constructor(sliderComponent: NzSliderComponent, cdr: ChangeDetectorRef);
    ngOnChanges(changes: SimpleChanges): void;
    ngOnDestroy(): void;
    enterHandle: () => void;
    leaveHandle: () => void;
    private toggleTooltip;
    private updateTooltipTitle;
    private updateTooltipPosition;
    private updateStyle;
}
