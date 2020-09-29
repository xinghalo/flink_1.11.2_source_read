import { ElementRef, OnChanges, OnInit, TemplateRef } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { NzSizeLDSType } from '../core/types/size';
import { NzListGrid } from './interface';
export declare class NzListComponent implements OnInit, OnChanges {
    private el;
    private updateHostClassService;
    nzDataSource: any[];
    nzBordered: boolean;
    nzGrid: NzListGrid;
    nzHeader: string | TemplateRef<void>;
    nzFooter: string | TemplateRef<void>;
    nzItemLayout: 'vertical' | 'horizontal';
    nzRenderItem: TemplateRef<void>;
    nzLoading: boolean;
    nzLoadMore: TemplateRef<void>;
    nzPagination: TemplateRef<void>;
    nzSize: NzSizeLDSType;
    nzSplit: boolean;
    nzNoResult: string | TemplateRef<void>;
    private prefixCls;
    private _setClassMap;
    constructor(el: ElementRef, updateHostClassService: NzUpdateHostClassService);
    ngOnInit(): void;
    ngOnChanges(): void;
}
