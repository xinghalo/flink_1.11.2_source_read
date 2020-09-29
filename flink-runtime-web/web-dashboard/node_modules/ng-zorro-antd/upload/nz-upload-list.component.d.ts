import { ChangeDetectorRef, ElementRef, OnChanges } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { ShowUploadListInterface, UploadFile, UploadListType } from './interface';
export declare class NzUploadListComponent implements OnChanges {
    private el;
    private cdr;
    private updateHostClassService;
    private imageTypes;
    private _items;
    readonly showPic: boolean;
    locale: any;
    listType: UploadListType;
    items: UploadFile[];
    icons: ShowUploadListInterface;
    onPreview: (file: UploadFile) => void;
    onRemove: (file: UploadFile) => void;
    private prefixCls;
    private setClassMap;
    private extname;
    isImageUrl(file: UploadFile): boolean;
    private previewFile;
    private genThumb;
    showPreview(file: UploadFile): boolean;
    handlePreview(file: UploadFile, e: Event): void;
    handleRemove(file: UploadFile, e: Event): void;
    constructor(el: ElementRef, cdr: ChangeDetectorRef, updateHostClassService: NzUpdateHostClassService);
    detectChanges(): void;
    ngOnChanges(): void;
}
