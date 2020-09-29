import { ChangeDetectorRef, OnChanges, OnInit, SimpleChanges, TemplateRef } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { FunctionProp } from '../core/types/common-wrap';
import { DateHelperService } from '../i18n/date-helper.service';
import { NzI18nService } from '../i18n/nz-i18n.service';
import { AbstractPickerComponent } from './abstract-picker.component';
import { CandyDate } from './lib/candy-date';
import { PanelMode } from './standard-types';
/**
 * The base picker for header panels, current support: Year/Month
 */
export declare class HeaderPickerComponent extends AbstractPickerComponent implements OnInit, OnChanges {
    nzPlaceHolder: string;
    nzRenderExtraFooter: FunctionProp<TemplateRef<void> | string>;
    nzDefaultValue: CandyDate;
    nzFormat: string;
    endPanelMode: SupportHeaderPanel;
    panelMode: PanelMode;
    extraFooter: TemplateRef<void> | string;
    private supportPanels;
    constructor(i18n: NzI18nService, cdr: ChangeDetectorRef, dateHelper: DateHelperService, noAnimation?: NzNoAnimationDirective);
    ngOnInit(): void;
    ngOnChanges(changes: SimpleChanges): void;
    onPanelModeChange(mode: PanelMode): void;
    onChooseValue(mode: SupportHeaderPanel, value: CandyDate): void;
    onOpenChange(open: boolean): void;
    private cleanUp;
}
export declare type SupportHeaderPanel = 'year' | 'month';
